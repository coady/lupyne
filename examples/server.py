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
from tests import conftest
Q = engine.Query


if __name__ == '__main__':
    assert lucene.getVMEnv() or lucene.initVM(vmargs='-Xrs')
    root = server.WebIndexer()
    # assign field settings
    root.indexer.set('amendment', engine.Field.String, stored=True)
    root.indexer.set('date', engine.Field.String, stored=True, docValuesType='sorted')
    root.indexer.set('text', engine.Field.Text)
    # populate index
    for doc in conftest.constitution():
        if 'amendment' in doc:
            root.indexer.add(amendment=doc['amendment'], date=doc['date'], text=doc['text'])
    root.update()
    # assign custom facet queries based on year
    years = {date.split('-')[0] for date in root.searcher.terms('date')}
    root.query_map['year'] = {year: Q.prefix('date', year) for year in years}
    # start server
    server.start(root)
