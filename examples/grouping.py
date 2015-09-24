"""
Grouping and facets.

Lupyne supports lucene's contrib grouping.GroupingSearch interface, but it has some limitations.
GroupingSearch objects only support single-valued strings, and won't find zero-valued facets.
Lupyne also supports grouping hits by an arbitrary function after the original search,
Similar to sorting, the native approach is generally more efficient, proportional to the number of documents culled.

Lupyne also supports using cached filters to compute facet counts.
Although seemingly less efficient, it is significantly faster with small numbers of terms.
It also has no limitations on multiple values, and can be fully customized without reindexing.
"""

import itertools
import lucene
from lupyne import engine
lucene.initVM()

colors = 'red', 'green', 'blue', 'cyan', 'magenta', 'yellow'
facets = dict(zip(colors, itertools.count(1)))
indexer = engine.Indexer()
indexer.set('color', stored=True, tokenized=False)
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
for hits in indexer.search(query).groupby(indexer.comparator('color').__getitem__, docs=1):
    assert facets[hits.value] == hits.count
    hit, = hits
    assert hit['color'] == hits.value

# facets use a GroupingSearch if no filters are registered
assert indexer.facets(query, 'color')['color'] == facets

# filters allow flexible customizations without any indexing changes
indexer.filters['color'] = {
    'additive': engine.Query.any(color=colors[:3]).filter(),
    'subtractive': engine.Query.any(color=colors[3:]).filter(),
}
assert indexer.facets(query, 'color')['color'] == {'additive': 6, 'subtractive': 15}
