"""
Pythonic wrapper around lucene search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives.
"""

import warnings
import lucene

if lucene.getVMEnv() is None:
    warnings.warn("lucene.initVM(lucene.CLASSPATH,... ) must be called before using lucene.", RuntimeWarning, stacklevel=2)

from indexers import Indexer, IndexWriter, IndexSearcher, IndexReader
