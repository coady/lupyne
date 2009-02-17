"""
Pythonic wrapper around `PyLucene <http://lucene.apache.org/pylucene/>`_ search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import warnings
import lucene

if lucene.getVMEnv() is None:
    warnings.warn("lucene.initVM(lucene.CLASSPATH,... ) must be called before using lucene.", RuntimeWarning, stacklevel=2)

from .queries import Query, Filter
from .documents import Document, Field, PrefixField, NestedField
from .indexers import Indexer, IndexSearcher
from .spatial import PointField, PolygonField

