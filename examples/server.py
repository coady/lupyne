"""
Custom server.

Fields settings are assigned directly to the root.
Indexing is done here just to populate the example.

A custom filter and sorter are demonstrated by transforming a date field into a year field.
Filters are also used for faceting;  sorters are also used for grouping.

Example queries:
 * http://localhost:8080/search?q=date:17*&group=year
 * http://localhost:8080/search?q=date:17*&group=year&sort=-year
 * http://localhost:8080/search?count=0&facets=year
 * http://localhost:8080/search?q=text:right&count=3&facets=year
"""

import lucene
from lupyne import engine, server
from tests import fixtures


def parse(date):
    return int(date.utf8ToString().split('-')[0])

if __name__ == '__main__':
    lucene.initVM(vmargs='-Xrs')
    root = server.WebIndexer()
    # assign field settings
    root.indexer.set('amendment', stored=True, tokenized=False)
    root.indexer.set('date', stored=True, tokenized=False)
    root.indexer.set('text')
    # populate index
    for doc in fixtures.constitution():
        if 'amendment' in doc:
            root.indexer.add(doc)
    root.update()
    # assign custom filter and sorter based on year
    root.searcher.sorters['year'] = engine.SortField('date', int, parse)
    years = {date.split('-')[0] for date in root.searcher.terms('date')}
    root.searcher.filters['year'] = {year: engine.Query.prefix('date', year).filter() for year in years}
    # start with pretty-printing
    server.start(root, config={'global': {'tools.json_out.indent': 2}})
