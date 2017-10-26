"""
PyLucene has several pitfalls when collecting or sorting a large query result.
Generally they involve the overhead of traversing the VM in an internal loop.

Lucene also requires supplying a maximum doc count for searches,
and supplying an excessively large count is a poor workaround because the collection heap is pre-allocated.

To mitigate these problems, Lupyne first provides a unified search interface.
The same Hits type is returned regardless of optional doc count or sorting parameters.
As with lucene, the result is fully evaluated but each individual Hit object will only be loaded on demand.
Internally a CachingCollector is used when all docs are requested.

The search method allows lucene Sort parameters to be passed through, since that's still optimal.
Additionally the hits themselves can be sorted afterwards with any python callable key.
The IndexReader.docvalues method is convenient for creating a sort key table from fields with docvalues.
The upshot is custom sorting and sorting large results are both easier and faster.

Custom sorting isn't necessary in the below example of course, just there for demonstration.
"""

import lucene
from org.apache.lucene import search
from lupyne import engine
assert lucene.getVMEnv() or lucene.initVM()

colors = 'red', 'green', 'blue', 'cyan', 'magenta', 'yellow'
indexer = engine.Indexer()
indexer.set('color', engine.Field.String, stored=True, docValuesType='sorted')
for color in colors:
    indexer.add(color=color)
indexer.commit()

# # # lucene # # #

searcher = search.IndexSearcher(indexer.indexReader)
sorter = search.Sort(search.SortField('color', search.SortField.Type.STRING))
topdocs = searcher.search(search.MatchAllDocsQuery(), 10, sorter)
assert [searcher.doc(scoredoc.doc)['color'] for scoredoc in topdocs.scoreDocs] == sorted(colors)

# # # lupyne # # #

hits = indexer.search(sort='color')
assert [hit['color'] for hit in hits] == sorted(colors)
docvalues = hits.docvalues('color')
assert docvalues == dict(enumerate(colors))
hits = indexer.search().sorted(docvalues.__getitem__)
assert [hit['color'] for hit in hits] == sorted(colors)
