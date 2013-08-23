import lucene
lucene.initVM(vmargs='-Djava.awt.headless=true')
from . import indexers, queries, searching, server, sorting
