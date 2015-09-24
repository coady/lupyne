"""
Parallel indexing.

One of Lucene's shortcomings as a general purpose database is the lack of atomic partial updates.
IndexWriter.updateDocument merely deletes and adds a document in a transaction.
The burden is on the application to handle both the inefficiency and concurrency issues of updating unchanged fields.
This is poorly suited for many scenarios, where there are large static fields (e.g. text) and small volatile fields (e.g. tags).
Thus many applications must keep volatile data in an external database, with poor performance when joining searches across vertical partitions.

Solutions have been discussed for years (https://issues.apache.org/jira/browse/LUCENE-1879) with little progress.
IndexWriters can now update DocValues in-place, but that's only a partial workaround since DocValues aren't indexed.
ParallelReaders allow keeping the volatile fields in a separate index, but require syncing the ephemeral doc nums.
This is essentially useless, as the whole point is that the indices wouldn't be updated with the same frequency.

Lupyne provides another solution: parallel indexing with syncing on a unique indexed field.
The most efficient way to intersect a search with outside data is to use a cached TermsFilter.
Lupyne's TermsFilter provides a set-like interface for managing which unique terms should match.
For simplicity and efficiency a searcher must also be registered with the filter before using it in a search.
The TermsFilter instance manages the thread-safe cache, with optimal incremental updates of both terms and searchers.

Additionally TermsFilters can be registered with IndexSearchers, such that reopening keeps the filter updated.
Finally, for applications which can also keep the volatile data in a separate Lucene index,
a ParallelIndexer will manage the matching terms by mapping the real underlying filters into terms,
keeping the registered TermsFilters updated with every commit.
"""

import lucene
from lupyne import engine
lucene.initVM()

# setup main index with unique name field
primary = engine.Indexer()
primary.set('name', stored=True, tokenized=False)
primary.set('text')
for name in ('alpha', 'bravo'):
    primary.add(name=name, text='large body of text')
primary.commit()

# setup parallel index with matching unique field and additional volatile field
secondary = engine.ParallelIndexer('name')
field = secondary.set('votes', engine.NumericField)
secondary.add(name='alpha', votes=1)
secondary.add(name='bravo', votes=0)
secondary.add(name='charlie', votes=1)
secondary.commit()

# automatically create and register TermsFilter, which matches positive votes
real_filter = engine.Query.filter(field.range(1, None), cache=False)
assert str(real_filter) == "votes:[1 TO *}"
auto_filter = secondary.termsfilter(real_filter, primary)

# instead of using parallel index, manually create and register TermsFilter
man_filter = primary.termsfilter('name', ['alpha', 'charlie'])

# in either case: alpha matches, bravo doesn't, charlie doesn't exist (yet)
for filter in (man_filter, auto_filter):
    assert [hit['name'] for hit in primary.search(filter=filter)] == ['alpha']

# update vote counts
secondary.update('alpha', votes=0)
secondary.update('bravo', votes=1)
secondary.commit()

# instead of using parallel index, simulate the updates manually
man_filter.discard('alpha')
man_filter.add('bravo')

# add missing document to main index
primary.add(name='charlie')
primary.commit()

# in either case: alpha no longer matches, bravo now does, charlie now exists
for filter in (man_filter, auto_filter):
    assert [hit['name'] for hit in primary.search(filter=filter)] == ['bravo', 'charlie']
