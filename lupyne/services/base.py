import time
from typing import List, Optional
import graphql
import lucene
import strawberry
from org.apache.lucene import index
from .settings import SCHEMA
from .. import engine

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
    return {name for name, tp in annotations.items() if getattr(tp, '__origin__', tp) is list}


@strawberry.type
class Document:
    """stored fields"""

    __annotations__ = {field.name.value: Optional[convert(field.type)] for field in schema.get('Document', [])}
    locals().update(dict.fromkeys(__annotations__))
    locals().update(dict.fromkeys(multi_valued(__annotations__), ()))

    def __init__(self, **doc):
        for name, values in doc.items():
            setattr(self, name, values[0] if getattr(type(self), name, ()) is None else values)


sort_types = {field.name.value: convert(field.type) for field in schema.get('FieldDoc', [])}


@strawberry.type
class FieldDoc:
    """sort fields"""

    __annotations__ = {name: Optional[sort_types[name]] for name in sort_types}
    locals().update(dict.fromkeys(__annotations__))
    assert not multi_valued(__annotations__)


class WebSearcher:
    """Dispatch root with a delegated Searcher."""

    def __init__(self, *directories, **kwargs):
        if len(directories) > 1:  # pragma: no cover
            self._searcher = engine.MultiSearcher(directories, **kwargs)
        else:
            self._searcher = engine.IndexSearcher(*directories, **kwargs)
        self.updated = time.time()

    def close(self):
        """Explicit close for clean shutdown."""
        del self._searcher  # pragma: no cover

    @property
    def searcher(self) -> engine.IndexSearcher:
        """attached IndexSearcher"""
        lucene.getVMEnv().attachCurrentThread()
        return self._searcher

    @property
    def etag(self) -> str:
        """ETag header"""
        return f'W/"{self.searcher.version}"'

    @property
    def age(self) -> float:
        """Age header"""
        return time.time() - self.updated

    def index(self) -> dict:
        """index information"""
        searcher = self.searcher
        if isinstance(searcher, engine.MultiSearcher):  # pragma: no cover
            return {reader.directory().toString(): reader.numDocs() for reader in searcher.indexReaders}
        return {searcher.directory.toString(): len(searcher)}

    def refresh(self, spellcheckers: bool = False) -> dict:
        """Refresh index version."""
        self._searcher = self.searcher.reopen(spellcheckers=spellcheckers)
        self.updated = time.time()
        return self.index()

    def indexed(self) -> list:
        """indexed field names"""
        fieldinfos = self.searcher.fieldinfos.values()
        return sorted(fi.name for fi in fieldinfos if fi.indexOptions != index.IndexOptions.NONE)

    def sortfields(self, sort: list) -> dict:
        """Return mapping of fields to lucene SortFields."""
        sort = {name.lstrip('-'): name.startswith('-') for name in sort}
        return {name: self.searcher.sortfield(name, sort_types[name], sort[name]) for name in sort}
