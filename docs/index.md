Lupyne is a search engine based on [PyLucene](http://lucene.apache.org/pylucene/), the Python extension for accessing Java Lucene.

## Quickstart
```python
>>> from lupyne import engine                       # don't forget to call lucene.initVM
>>> indexer = engine.Indexer('temp')                # create an index at path
>>> indexer.set('name', stored=True)                # create stored 'name' field
>>> indexer.set('text', engine.Field.Text)          # create indexed 'text' field
>>> indexer.add(name='sample', text='hello world')  # add a document to the index
>>> indexer.commit()                                # commit changes; document is now searchable
>>> hits = indexer.search('text:hello')             # run search and return sequence of documents
>>> len(hits), hits.count                           # 1 hit retrieved (out of a total of 1)
(1, 1)
>>> (hit,) = hits
>>> hit['name']                                     # hits support mapping interface for their stored fields
'sample'
>>> hit.id, hit.score                               # plus internal doc number and score
(0, 0.28768208622932434)
>>> hit.dict()                                      # dict representation of the hit document
{'name': 'sample', '__id__': 0, '__score__': 0.28768208622932434}
```
