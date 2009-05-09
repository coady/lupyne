"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

from .queries import Query, Filter
from .documents import Document, Field, PrefixField, NestedField
from .indexers import IndexSearcher, IndexWriter, Indexer
from .spatial import PointField, PolygonField
