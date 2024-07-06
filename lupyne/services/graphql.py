import contextlib
import functools
import inspect
import math
from collections.abc import Callable
from typing import Annotated, Optional
import lucene
import strawberry.asgi
from starlette.applications import Starlette
from strawberry import Info, UNSET
from .settings import DEBUG, DIRECTORIES
from .base import Document, FieldDoc, WebSearcher


@contextlib.asynccontextmanager
async def lifespan(app: Starlette):  # pragma: no cover
    yield
    root.close()


assert lucene.getVMEnv() or lucene.initVM()
root = WebSearcher(*DIRECTORIES)
app = Starlette(debug=DEBUG, lifespan=lifespan)


def selections(*fields) -> dict:
    """Return tree of field name selections."""
    return {
        selection.name: selections(selection) for field in fields for selection in field.selections
    }


def doc_type(cls):
    """Return strawberry type with docstring descriptions."""
    return strawberry.type(cls, description=inspect.getdoc(cls))


def doc_field(func: Optional[Callable] = None, **kwargs: str):
    """Return strawberry field with argument and docstring descriptions."""
    if func is None:
        return functools.partial(doc_field, **kwargs)
    for name in kwargs:
        argument = strawberry.argument(description=kwargs[name])
        func.__annotations__[name] = Annotated[func.__annotations__[name], argument]
    return strawberry.field(func, description=inspect.getdoc(func))


@doc_type
class Index:
    """index information"""

    directories: list[str]
    counts: list[int]


@doc_type
class Terms:
    """terms and counts"""

    values: list[str]
    counts: list[int] = ()


@doc_type
class IndexedFields:
    """indexed field names"""

    __annotations__ = {name: Terms for name in root.indexed()}
    locals().update(dict.fromkeys(__annotations__, UNSET))


@doc_type
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


@doc_type
class Hits:
    """search results"""

    count: int
    hits: list[Hit]


@doc_type
class Query:
    @doc_field
    def index(self) -> Index:
        """index information"""
        index = root.index()
        return Index(directories=list(index), counts=index.values())

    @doc_field
    def terms(self, info: Info) -> IndexedFields:
        """indexed field names"""
        fields = {}
        for name, selected in selections(*info.selected_fields).items():
            if 'counts' in selected:
                terms = dict(root.searcher.terms(name, counts=True))
                fields[name] = Terms(values=terms, counts=terms.values())
            else:
                fields[name] = Terms(values=root.searcher.terms(name))
        return IndexedFields(**fields)

    @doc_field(
        q="query string",
        count="maximum number of hits to retrieve",
        sort="sort by fields",
    )
    def search(self, info: Info, q: str, count: Optional[int] = None, sort: list[str] = []) -> Hits:
        """Run query and return hits."""
        selected = selections(*info.selected_fields)
        if 'hits' not in selected or count == 0:
            return Hits(count=root.searcher.count(q), hits=[])
        sortfields = root.sortfields(sort)
        hits = root.searcher.search(
            q,
            count,
            sort=list(sortfields.values()) or None,
            scores='score' in selected['hits'],
        )
        hits.select(*selected['hits'].get('doc', []))
        result = Hits(count=hits.count, hits=[])
        for hit in hits:
            sortkeys = dict(zip(sortfields, hit.sortkeys))
            result.hits.append(Hit(hit.id, hit.score, sortkeys, hit))
        return result


@doc_type
class Mutation:
    @doc_field
    def index(self) -> Index:
        """Refresh index."""
        index = root.refresh()
        return Index(directories=list(index), counts=index.values())


schema = strawberry.Schema(query=Query, mutation=Mutation)
app.add_route('/graphql', strawberry.asgi.GraphQL(schema, debug=DEBUG))
