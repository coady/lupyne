"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import types
import warnings
warnings.simplefilter('default', DeprecationWarning)

from .queries import Query, Filter, SortField
from .documents import Document, Field, FormatField, PrefixField, NestedField, NumericField, DateTimeField
from .indexers import TokenFilter, Analyzer, IndexSearcher, MultiSearcher, ParallelMultiSearcher, IndexWriter, Indexer
from .spatial import PointField, PolygonField

class numeric(types.ModuleType):
    def __getattr__(self, name):
        warnings.warn('The numeric module has been removed; access numeric fields from engine package.', DeprecationWarning)
        return globals()[name]
numeric = numeric('numeric')
