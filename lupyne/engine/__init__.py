"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import warnings, lucene
if lucene.VERSION <= '2.4.1':
    warnings.warn('Support for lucene 2.4 will be removed in the next release.', DeprecationWarning)

from .queries import Query, Filter
from .documents import Document, Field, FormatField, PrefixField, NestedField, DateTimeField
from .indexers import TokenFilter, Analyzer, IndexSearcher, MultiSearcher, ParallelMultiSearcher, IndexWriter, Indexer
from .spatial import PointField, PolygonField
from . import numeric
