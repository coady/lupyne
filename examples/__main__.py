import lucene
lucene.initVM(vmargs='-Djava.awt.headless=true')
from . import grouping, indexers, queries, searching, server, sorting
