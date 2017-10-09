"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import lucene  # flake8: noqa
from .queries import Query, SortField
from .documents import Document, Field, NestedField, NumericField, DateTimeField
from .indexers import TokenFilter, Analyzer, IndexSearcher, MultiSearcher, IndexWriter, Indexer
from .spatial import PointField, PolygonField

version = tuple(map(int, lucene.VERSION.split('.')))
assert version >= (4, 10)
