import lucene
lucene.initVM(vmargs='-Djava.awt.headless=true')
from . import grouping, indexers, parallel, queries, searching, sorting
