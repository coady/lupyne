from typing import List, Optional
import graphql
import lucene
import strawberry.asgi
from starlette.applications import Starlette
from .settings import DEBUG, DIRECTORIES, SCHEMA
from .base import WebSearcher

assert lucene.getVMEnv() or lucene.initVM()
root = WebSearcher(*DIRECTORIES)
app = Starlette(debug=DEBUG)
app.on_event('shutdown')(root.close)

type_map = {
    'Int': int,
    'Float': float,
    'String': str,
}
schema = {}
if SCHEMA:  # pragma: no branch
    document = graphql.parse(open(SCHEMA).read())
    schema = {definition.name.value: definition.fields for definition in document.definitions}


def convert(node):
    """Return type annotation from graphql node."""
    if isinstance(node, graphql.NonNullTypeNode):
        return convert(node.type)
    if isinstance(node, graphql.ListTypeNode):  # pragma: no cover
        return List[convert(node.type)]
    return type_map[node.name.value]


def multi_valued(annotations):
    """Return set of multi-valued fields."""
    return {name for name, tp in annotations.items() if getattr(tp, '__origin__', 'tp') is list}


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
    counts: List[int] = ()


@strawberry.type
class IndexedFields:
    """indexed field names"""

    __annotations__ = {name: Terms for name in root.indexed()}
    locals().update(dict.fromkeys(__annotations__, graphql.Undefined))


@strawberry.type
class Document:
    """stored fields"""

    __annotations__ = {field.name.value: convert(field.type) for field in schema.get('Document', [])}
    locals().update(dict.fromkeys(__annotations__))
    locals().update(dict.fromkeys(multi_valued(__annotations__), ()))

    def __init__(self, **doc):
        for name, values in doc.items():
            setattr(self, name, values[0] if getattr(type(self), name) is None else values)


@strawberry.type
class FieldDoc:
    """sort fields"""

    __annotations__ = {field.name.value: convert(field.type) for field in schema.get('FieldDoc', [])}
    locals().update(dict.fromkeys(__annotations__))
    assert not multi_valued(__annotations__)


@strawberry.type
class Hit:
    """search result"""

    id: int
    score: Optional[float]
    sortkeys: FieldDoc
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
            fields[name] = Terms(*zip(*terms)) if counts else Terms(terms)
        return IndexedFields(**fields)

    @strawberry.field
    def search(self, info, q: str, count: int = None, sort: List[str] = []) -> Hits:
        """Run query and return hits."""
        sort = {name.lstrip('-'): name.startswith('-') for name in sort}
        sortfields = [root.searcher.sortfield(name, FieldDoc.__annotations__[name], sort[name]) for name in sort]
        hits = root.searcher.search(q, count, sortfields or None)
        hits.select(*selections(*info.field_nodes).get('hits', {}).get('doc', []))
        result = Hits(hits.count, [])
        for hit in hits:
            sortkeys = FieldDoc(**dict(zip(sort, hit.sortkeys)))
            result.hits.append(Hit(hit.id, None if sort else hit.score, sortkeys, Document(**hit)))
        return result


@strawberry.type
class Mutation:
    @strawberry.field
    def index(self, spellcheckers: bool = False) -> Index:
        """Refresh index."""
        index = root.refresh(spellcheckers=spellcheckers)
        return Index(directories=index, counts=index.values())


schema = strawberry.Schema(query=Query, mutation=Mutation)
app.add_route('/graphql', strawberry.asgi.GraphQL(schema, debug=DEBUG))
