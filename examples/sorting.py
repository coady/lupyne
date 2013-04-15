"""
PyLucene has several pitfalls when collecting or sorting a large query result.
Generally they involve the overhead of traversing the VM in an internal loop.

Lucene also requires supplying a maximum doc count for searches,
and supplying an excessively large count is a poor workaround because the collection heap is pre-allocated.

Finally the custom sorting interface, although well-supported in PyLucene, has horrible performance.
The sort key of every potential doc must realistically be cached,
but the overhead of O(n log n) comparison calls dispatched through the VM is far worse than iterating ScoreDocs.

To mitigate all these problems, Lupyne first provides a unified search interface.
The same Hits type is returned regardless of optional doc count or sorting parameters.
As with lucene, the result is fully evaluated but each individual Hit object will only be loaded on demand.
Internally a CachingCollector is used when all docs are requested.

The search method allows lucene Sort parameters to be passed through, since that's still optimal.
Additionally the hits themselves can be sorted afterwards with any python callable key.
The IndexSearcher.comparator method is convenient for creating a sort key table from indexed fields.
The upshot is custom sorting and sorting large results are both easier and faster.

Custom sorting isn't necessary in the below example of course, just there for demonstration.
"""

import lucene
lucene.initVM()
try:
    from org.apache.lucene import search, util
    from org.apache.pylucene.search import PythonFieldComparator, PythonFieldComparatorSource
except ImportError:
    search = util = lucene
    from lucene import PythonFieldComparator, PythonFieldComparatorSource
from lupyne import engine

colors = 'red', 'green', 'blue', 'cyan', 'magenta', 'yellow'
indexer = engine.Indexer()
indexer.set('color', store=True, index=True)
for color in colors:
    indexer.add(color=color)
indexer.commit()

### lucene ###

searcher = search.IndexSearcher(indexer.indexReader)
sorttype = getattr(search.SortField, 'Type', search.SortField).STRING
topdocs = searcher.search(search.MatchAllDocsQuery(), None, 10, search.Sort(search.SortField('color', sorttype)))
assert [searcher.doc(scoredoc.doc)['color'] for scoredoc in topdocs.scoreDocs] == sorted(colors)

class ComparatorSource(PythonFieldComparatorSource):
    class newComparator(PythonFieldComparator):
        def __init__(self, name, numHits, sortPos, reversed):
            PythonFieldComparator.__init__(self)
            self.name = name
            self.values = [None] * numHits
        def setNextReader(self, reader, *args):
            if not args:
                reader = reader.reader()
            if hasattr(search.FieldCache, 'getStrings'):
                self.comparator = search.FieldCache.DEFAULT.getStrings(reader, self.name)
            else:
                br = util.BytesRef()
                comparator = search.FieldCache.DEFAULT.getTerms(reader, self.name)
                self.comparator = [comparator.get(id, br) or br.utf8ToString() for id in range(reader.maxDoc())]
            return self
        def compare(self, slot1, slot2):
            return cmp(self.values[slot1], self.values[slot2])
        def setBottom(self, slot):
            self._bottom = self.values[slot]
        def compareBottom(self, doc):
            return cmp(self._bottom, self.comparator[doc])
        def copy(self, slot, doc):
            self.values[slot] = self.comparator[doc]
        def value(self, slot):
            pass

sorter = search.Sort(search.SortField('color', ComparatorSource()))
# still must supply excessive doc count to use the sorter
topdocs = searcher.search(search.MatchAllDocsQuery(), None, 10, sorter)
assert [searcher.doc(scoredoc.doc)['color'] for scoredoc in topdocs.scoreDocs] == sorted(colors)

### lupyne ###

hits = indexer.search(sort='color')
assert [hit['color'] for hit in hits] == sorted(colors)
comparator = indexer.comparator('color')
assert list(comparator) == list(colors)
hits = indexer.search().sorted(comparator.__getitem__)
assert [hit['color'] for hit in hits] == sorted(colors)
