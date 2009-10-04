"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

from .queries import Query, Filter
from .documents import Document, Field, FormatField, PrefixField, NestedField, DateTimeField
from .indexers import Analyzer, IndexSearcher, MultiSearcher, ParallelMultiSearcher, IndexWriter, Indexer
from .spatial import PointField, PolygonField

import lucene
if hasattr(lucene, 'NumericField'):
    from .documents import NumericField
