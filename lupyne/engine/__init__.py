"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import lucene  # flake8: noqa
from .analyzers import Analyzer, TokenFilter
from .queries import Query
from .documents import Document, Field, NestedField, NumericField, DateTimeField
from .indexers import IndexSearcher, MultiSearcher, IndexWriter, Indexer
from .spatial import PointField

version = tuple(map(int, lucene.VERSION.split('.')))
assert version >= (6,), version
