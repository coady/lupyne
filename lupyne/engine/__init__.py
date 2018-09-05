"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import lucene  # flake8: noqa
from .analyzers import Analyzer, TokenFilter
from .queries import Query
from .documents import Document, Field, NestedField, DateTimeField, SpatialField
from .indexers import IndexSearcher, MultiSearcher, IndexWriter, Indexer

version = tuple(map(int, lucene.VERSION.split('.')))
assert version >= (7,), version
