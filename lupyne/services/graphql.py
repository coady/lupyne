import math
from typing import List, Optional
import graphql
import lucene
import strawberry.asgi
from starlette.applications import Starlette
from strawberry.types import Info
from .settings import DEBUG, DIRECTORIES
from .base import Document, FieldDoc, WebSearcher

assert lucene.getVMEnv() or lucene.initVM()
root = WebSearcher(*DIRECTORIES)
app = Starlette(debug=DEBUG)
app.on_event('shutdown')(root.close)


def selections(*fields) -> dict:
    """Return tree of field name selections."""
    return {selection.name: selections(selection) for field in fields for selection in field.selections}


@strawberry.type
class Index:
    """index information"""

    directories: List[str]
    counts: List[int]


@strawberry.type
class Terms:
    """terms and counts"""

    values: List[str]
    counts: List[int] = ()


@strawberry.type
class IndexedFields:
    """indexed field names"""

    __annotations__ = {name: Terms for name in root.indexed()}
    locals().update(dict.fromkeys(__annotations__, graphql.Undefined))


@strawberry.type
class Hit:
    """search result"""

    id: int
    score: Optional[float]
    if FieldDoc.__annotations__:  # pragma: no branch
        sortkeys: FieldDoc
    if Document.__annotations__:  # pragma: no branch
        doc: Document

    def __init__(self, id, score, sortkeys=None, doc=None):
        self.id = id
        self.score = None if math.isnan(score) else score
        if sortkeys is not None:  # pragma: no branch
            self.sortkeys = FieldDoc(**sortkeys)
        if doc is not None:  # pragma: no branch
            self.doc = Document(**doc)


@strawberry.type
class Hits:
    """search results"""

    count: int
    hits: List[Hit]


@strawberry.type
class Query:
    @strawberry.field
    def index(self) -> Index:
        """index information"""
        index = root.index()
        return Index(directories=list(index), counts=index.values())

    @strawberry.field
    def terms(self, info: Info) -> IndexedFields:
        """indexed field names"""
        fields = {}
        for name, selected in selections(*info.selected_fields).items():
            counts = 'counts' in selected
            terms = root.searcher.terms(name, counts=counts)
            fields[name] = Terms(**dict(zip(['values', 'counts'], zip(*terms)))) if counts else Terms(values=terms)
        return IndexedFields(**fields)

    @strawberry.field
    def search(self, info: Info, q: str, count: int = None, sort: List[str] = []) -> Hits:
        """Run query and return hits."""
        sortfields = root.sortfields(sort)
        hits = root.searcher.search(q, count, list(sortfields.values()) or None)
        hits.select(*selections(*info.selected_fields).get('hits', {}).get('doc', []))
        result = Hits(count=hits.count, hits=[])
        for hit in hits:
            sortkeys = dict(zip(sortfields, hit.sortkeys))
            result.hits.append(Hit(hit.id, hit.score, sortkeys, hit))
        return result


@strawberry.type
class Mutation:
    @strawberry.field
    def index(self, spellcheckers: bool = False) -> Index:
        """Refresh index."""
        index = root.refresh(spellcheckers=spellcheckers)
        return Index(directories=list(index), counts=index.values())


schema = strawberry.Schema(query=Query, mutation=Mutation)
app.add_route('/graphql', strawberry.asgi.GraphQL(schema, debug=DEBUG))
