"""
Example server.

Fields settings are assigned directly to the root.
Indexing is done here just to populate the example.

Registered queries demonstrate custom facets.

Example queries:
 * http://localhost:8080/search?q=date:17*&group=date
 * http://localhost:8080/search?count=0&facets=year
 * http://localhost:8080/search?q=text:right&count=3&facets=year
"""

import lucene
from lupyne import engine, server
from tests import fixtures
Q = engine.Query


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
    # assign custom facet queries based on year
    years = {date.split('-')[0] for date in root.searcher.terms('date')}
    root.query_map['year'] = {year: Q.prefix('date', year) for year in years}
    # start with pretty-printing
    server.start(root, config={'global': {'tools.json_out.indent': 2}})
