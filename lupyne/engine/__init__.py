"""
Pythonic wrapper around [PyLucene](http://lucene.apache.org/pylucene/) search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import lucene

from .analyzers import Analyzer, TokenFilter
from .documents import DateTimeField, Document, Field, NestedField, ShapeField
from .indexers import Indexer, IndexSearcher, IndexWriter, MultiSearcher
from .queries import Query

version = tuple(map(int, lucene.VERSION.split(".")))
assert version >= (10,), version
