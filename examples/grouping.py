"""
Grouping and facets.

Lupyne supports lucene's contrib grouping.GroupingSearch interface, but it has some limitations.
GroupingSearch objects only support single-valued strings, and won't find zero-valued facets.
Lupyne also supports grouping hits by an arbitrary function after the original search,
Similar to sorting, the native approach is generally more efficient, proportional to the number of documents culled.

Lupyne can also compute facet counts with intersected queries.
Although seemingly less efficient, it may be faster with small numbers of terms.
It also has no limitations on multiple values, and can be fully customized without reindexing.
"""

import itertools
import lucene
from lupyne import engine
assert lucene.getVMEnv() or lucene.initVM()

colors = 'red', 'green', 'blue', 'cyan', 'magenta', 'yellow'
facets = dict(zip(colors, itertools.count(1)))
indexer = engine.Indexer()
indexer.set('color', engine.Field.String, stored=True, docValuesType='sorted')
for color in facets:
    for index in range(facets[color]):
        indexer.add(color=color)
indexer.commit()
query = engine.Query.alldocs()

# group using native GroupingSearch
for hits in indexer.groupby('color', query):
    assert facets[hits.value] == hits.count
    hit, = hits
    assert hit['color'] == hits.value

# group using Hits interface
hits = indexer.search(query)
for hits in hits.groupby(hits.docvalues('color').__getitem__, docs=1):
    assert facets[hits.value] == hits.count
    hit, = hits
    assert hit['color'] == hits.value

# facets use a GroupingSearch when given fields
assert indexer.facets(query, 'color')['color'] == facets

# queries allow flexible customizations without any indexing changes
queries = {
    'additive': engine.Query.any(color=colors[:3]),
    'subtractive': engine.Query.any(color=colors[3:]),
}
assert indexer.facets(query, color=queries)['color'] == {'additive': 6, 'subtractive': 15}
