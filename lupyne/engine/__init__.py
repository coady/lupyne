"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import lucene
from .queries import Query, SortField, TermsFilter
from .documents import Document, Field, MapField, NestedField, DocValuesField, NumericField, DateTimeField
from .indexers import TokenFilter, Analyzer, IndexSearcher, MultiSearcher, IndexWriter, Indexer, ParallelIndexer
from .spatial import PointField, PolygonField

version = tuple(map(int, lucene.VERSION.split('.')))
assert version >= (4, 10)
