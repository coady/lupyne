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
    directories: List[str]
    counts: List[int]


@strawberry.type
class Values:
    __annotations__ = {name: List[str] for name in root.indexed()}
    locals().update(dict.fromkeys(__annotations__, ()))


@strawberry.type
class Counts:
    __annotations__ = {name: List[int] for name in root.indexed()}
    locals().update(dict.fromkeys(__annotations__, ()))


@strawberry.type
class Terms:
    values: Values
    counts: Counts


@strawberry.type
class Document:
    __annotations__ = {name: List[str] for name in root.searcher.fieldinfos}
    locals().update(dict.fromkeys(__annotations__, ()))


@strawberry.type
class Hit:
    id: int
    score: Optional[float]
    sortkeys: List[str]
    doc: Document


@strawberry.type
class Hits:
    count: int
    hits: List[Hit]


@strawberry.type
class Query:
    @strawberry.field
    def index(self, info) -> Index:
        """index information"""
        index = root.index()
        return Index(directories=index, counts=index.values())

    @strawberry.field
    def terms(self, info) -> Terms:
        """indexed field names"""
        names = selections(*info.field_nodes)
        counts = dict.fromkeys(names.get('counts', []))
        values = dict.fromkeys(set(names.get('values', [])) - set(counts))
        for name in values:
            values[name] = root.searcher.terms(name)
        for name in counts:
            values[name], counts[name] = zip(*root.searcher.terms(name, counts=True))
        return Terms(Values(**values), Counts(**counts))

    @strawberry.field
    def search(self, info, q: str, count: int = None) -> Hits:
        """Run query and return htis."""
        hits = root.searcher.search(q, count)
        return Hits(hits.count, hits=[Hit(hit.id, hit.score, hit.sortkeys, Document(**hit)) for hit in hits])


@strawberry.type
class Mutation:
    @strawberry.field
    def index(self, info, spellcheckers: bool = False) -> Index:
        """Refresh index."""
        index = root.refresh(spellcheckers=spellcheckers)
        return Index(directories=index, counts=index.values())


schema = strawberry.Schema(query=Query, mutation=Mutation)
app.add_route('/graphql', strawberry.asgi.GraphQL(schema, debug=DEBUG))
