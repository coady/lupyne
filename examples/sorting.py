"""
PyLucene has several pitfalls when collecting or sorting a large query result.
Generally they involve the overhead of traversing the VM in an internal loop.

Lucene's performance itself drops off noticeably as the requested doc count increases.
This is heavily compounded by having to iterate through a large set of ScoreDocs in PyLucene.

Lucene also only supports sorting a query result when a doc count is supplied.
And supplying an excessively large count is not a good workaround because of the aforementioned problem.

Finally the custom sorting interface, although well-supported in PyLucene, is bascially useless.
The sort key of every potential doc must realistically be cached anyway,
but the performance overhead of O(n log n) comparison calls in java is still horrid.

To mitigate all these problems, Lupyne first provides a unified search interface.
The same Hits type is returned regardless of whether a doc count is supplied.
As with lucene, the result is fully evaluated but each individual Hit object will only be loaded on demand.
Internally an optimized custom hit Collector is used when all docs are requested.

The search method does allow lucene Sort parameters to be passed through, since that's still optimal.
So the only gotcha is that with no doc count the sort parameter must instead be a python callable key.
The IndexReader.comparator method is convenient for creating a sort key table from indexed fields.
The upshot is custom sorting and sorting large results are both easier and faster.

Custom sorting isn't necessary in the below example of course, just there for demonstration.
Compatible with lucene versions 2.9 through 3.0.
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
topdocs = searcher.search(lucene.MatchAllDocsQuery(), None, 10, lucene.Sort(lucene.SortField('color', lucene.SortField.STRING)))
assert [searcher.doc(scoredoc.doc)['color'] for scoredoc in topdocs.scoreDocs] == sorted(colors)

class ComparatorSource(lucene.PythonFieldComparatorSource):
    class newComparator(lucene.PythonFieldComparator):
        def __init__(self, name, numHits, sortPos, reversed):
            lucene.PythonFieldComparator.__init__(self)
            self.name = name
            self.values = [None] * numHits
        def setNextReader(self, reader, base):
            self.comparator = lucene.FieldCache.DEFAULT.getStrings(reader, self.name)
        def compare(self, slot1, slot2):
            return cmp(self.values[slot1], self.values[slot2])
        def setBottom(self, slot):
            self._bottom = self.values[slot]
        def compareBottom(self, doc):
            return cmp(self._bottom, self.comparator[doc])
        def copy(self, slot, doc):
            self.values[slot] = self.comparator[doc]
        def value(self, slot):
            return lucene.String()

sorter = lucene.Sort(lucene.SortField('color', ComparatorSource()))
# still must supply excessive doc count to use the sorter
topdocs = searcher.search(lucene.MatchAllDocsQuery(), None, 10, sorter)
assert [searcher.doc(scoredoc.doc)['color'] for scoredoc in topdocs.scoreDocs] == sorted(colors)

### lupyne ###

hits = indexer.search(count=10, sort='color')
assert [hit['color'] for hit in hits] == sorted(colors)
comparator = indexer.comparator('color')
assert list(comparator) == list(colors)
hits = indexer.search(sort=comparator.__getitem__)
assert [hit['color'] for hit in hits] == sorted(colors)
