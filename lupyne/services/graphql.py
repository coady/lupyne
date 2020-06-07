from typing import List, Optional
import lucene
import strawberry.asgi
from starlette.applications import Starlette
from .settings import DEBUG, DIRECTORIES
from .base import WebSearcher

assert lucene.getVMEnv() or lucene.initVM()
root = WebSearcher(*DIRECTORIES)
app = Starlette(debug=DEBUG)
app.on_event('shutdown')(root.close)


def selections(node):
    """Return tree of field name selections."""
    nodes = getattr(node.selection_set, 'selections', [])
    return {node.name.value: selections(node) for node in nodes}


@strawberry.type
class Index:
    """index information"""

    directories: List[str]
    counts: List[int]


@strawberry.type
class Terms:
    """terms and counts"""

    values: List[str]
    counts: List[int]


@strawberry.type
class IndexedFields:
    """indexed field names"""

    __annotations__ = {name: Terms for name in root.indexed()}
    locals().update(dict.fromkeys(__annotations__))


@strawberry.type
class Document:
    """stored fields"""

    __annotations__ = {name: List[str] for name in root.searcher.fieldinfos}
    locals().update(dict.fromkeys(__annotations__))


@strawberry.type
class Hit:
    """search result"""

    id: int
    score: Optional[float]
    sortkeys: List[str]
    doc: Document


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
        return Index(directories=index, counts=index.values())

    @strawberry.field
    def terms(self, info) -> IndexedFields:
        """indexed field names"""
        fields = {}
        for name, selected in selections(*info.field_nodes).items():
            counts = 'counts' in selected
            terms = root.searcher.terms(name, counts=counts)
            fields[name] = Terms(*zip(*terms)) if counts else Terms(terms, [])
        return IndexedFields(**fields)

    @strawberry.field
    def search(self, q: str, count: int = None) -> Hits:
        """Run query and return hits."""
        hits = root.searcher.search(q, count)
        return Hits(hits.count, hits=[Hit(hit.id, hit.score, hit.sortkeys, Document(**hit)) for hit in hits])


@strawberry.type
class Mutation:
    @strawberry.field
    def index(self, spellcheckers: bool = False) -> Index:
        """Refresh index."""
        index = root.refresh(spellcheckers=spellcheckers)
        return Index(directories=index, counts=index.values())


schema = strawberry.Schema(query=Query, mutation=Mutation)
app.add_route('/graphql', strawberry.asgi.GraphQL(schema, debug=DEBUG))
