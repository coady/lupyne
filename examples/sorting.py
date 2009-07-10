"""
PyLucene has several pitfalls when collecting or sorting a large query result.
Generally they involve the overhead of traversing the VM in an internal loop.

Lucene's performance itself drops off noticeably as the requested doc count increases.
This is heavily compounded by having to iterate through a large set of ScoreDocs in PyLucene.

Lucene also only supports sorting a query result when a doc count is supplied (deprecation notwithstanding).
And supplying an excessively large count is not a good workaround because of the previous problem.

Finally the custom sorting interface, although supported in PyLucene, is bascially useless.
The sort key of every potential doc must realistically be cached anyway,
but the performance overhead of O(n log n) comparison calls in java is still horrid.

To mitigate all these problems, LuPyne first provides a unified search interface.
The same Hits type is returned regardless of whether a doc count is supplied.
As with lucene, the result is fully evaluated but each individual Hit object will only be loaded on demand.
Internally an optimized custom HitCollector is used when all docs are requested.

The search method does allow lucene Sort parameters to be passed through, since that's still optimal.
So the only gotcha is that with no doc count the sort parameter must instead be a python callable key.
The IndexReader.comparator method is convenient for creating a sort key table from indexed fields.
The upshot is custom sorting and sorting large results are both easier and faster.

Custom sorting isn't necessary in the below example of course, just there for demonstration.
"""

import lucene
lucene.initVM(lucene.CLASSPATH)
from lupyne import engine

colors = 'red', 'green', 'blue', 'cyan', 'magenta', 'yellow'
indexer = engine.Indexer()
indexer.set('color', store=True, index=True)
for color in colors:
    indexer.add(color=color)
indexer.commit()

### lucene ###

searcher = lucene.IndexSearcher(indexer.directory)
topdocs = searcher.search(lucene.MatchAllDocsQuery(), None, 10, lucene.Sort('color'))
assert [searcher.doc(scoredoc.doc)['color'] for scoredoc in topdocs.scoreDocs] == sorted(colors)

class SortComparatorSource(lucene.PythonSortComparatorSource):
    class newComparator(lucene.PythonScoreDocComparator):
        def __init__(self, reader, name):
            lucene.PythonScoreDocComparator.__init__(self)
            terms = reader.terms(lucene.Term(name, ''))
            self.comparator = [None] * reader.numDocs()
            from contextlib import closing
            with closing(reader.terms(lucene.Term(name, ''))) as terms:
                with closing(reader.termDocs()) as termDocs:
                    while True:
                        term = terms.term()
                        if term is None or term.field() != name:
                            break
                        termDocs.seek(term)
                        value = term.text()
                        while termDocs.next():
                            self.comparator[termDocs.doc()] = value
                        if not terms.next():
                            break
        def compare(self, i, j):
            return cmp(self.comparator[i.doc], self.comparator[j.doc])
        def sortValue(self, i):
            return lucene.String()
        def sortType(self):
            return lucene.SortField.STRING

sorter = lucene.Sort(lucene.SortField('color', SortComparatorSource()))
# still must supply excessive doc count to use the sorter
topdocs = searcher.search(lucene.MatchAllDocsQuery(), None, 10, sorter)
assert [searcher.doc(scoredoc.doc)['color'] for scoredoc in topdocs.scoreDocs] == sorted(colors)

### lupyne ###

hits = indexer.search(count=10, sort='color')
assert [hit['color'] for hit in hits] == sorted(colors)
comparator = indexer.comparator('color')
assert comparator == list(colors)
hits = indexer.search(sort=comparator.__getitem__)
assert [hit['color'] for hit in hits] == sorted(colors)
