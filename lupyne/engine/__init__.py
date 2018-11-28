"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import lucene
from .analyzers import Analyzer, TokenFilter  # noqa
from .queries import Query  # noqa
from .documents import Document, Field, NestedField, DateTimeField, SpatialField  # noqa
from .indexers import IndexSearcher, MultiSearcher, IndexWriter, Indexer  # noqa

version = tuple(map(int, lucene.VERSION.split('.')))
assert version >= (7,), version
