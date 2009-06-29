"""
Advanced searching with custom fields.

Prefix and Range queries are a potential pitfall in Lucene.
As the queries expand to more terms, the performance drops off precipitously.

A common example is where datetimes are indexed, but a large span of date ranges are being searched.
The usual workaround is to only index the amount of granularity needed, e.g., just the dates.
But this may not be sufficient, or the datetimes may be necessary for other searches.

In any case the principle can be generalized to indexing every prefix of a term.
The cost in indexing time and space is well worth the optimal search times.

LuPyne's PrefixField automates the indexing of such prefix trees into different fields.
The default naming convention makes each field look like a python slice of the original field.
The fields also provide prefix and range query generators that optimally utilize the underlying fields.

NestedFields extend PrefixFields to support a common separator.
DateTimeFields extend PrefixFields with datetime specific query generators.
"""

import lucene
lucene.initVM(lucene.CLASSPATH)
from lupyne import engine

docs = [
    {'city': 'San Francisco', 'state': 'CA', 'incorporated': '1850-04-15', 'population': '0,808,976'},
    {'city': 'Los Angeles', 'state': 'CA', 'incorporated': '1850-04-04', 'population': '3,849,378'},
    {'city': 'Portland', 'state': 'OR', 'incorporated': '1851-02-08', 'population': '0,575,930'},
]

indexer = engine.Indexer()
indexer.set('city', store=True, index=False)
indexer.set('state', store=True, index=False)
# set method supports custom field types inheriting their default settings
indexer.set('incorporated', engine.DateTimeField)
indexer.set('population', engine.PrefixField)
# assigned fields can have a different key from their underlying field name
indexer.fields['location'] = engine.NestedField('state:city')

for doc in docs:
    location = doc['state'] + ':' + doc['city']
    indexer.add(doc, location=location)
indexer.commit()

query = indexer.fields['incorporated'].range('1800', '1851-02-07')
# automatically handles date arithmetic to compute an optimal boolean (OR) query
assert str(query) == 'incorporated:Y:[1800 TO 1851} incorporated:Ym:[1851 TO 1851-02} incorporated:Ymd:[1851-02 TO 1851-02-07}'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Los Angeles']

query = indexer.fields['population'].range('0', '1,000,000')
# works like any range query
assert str(query) == 'population[:9]:[0 TO 1,000,000}'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Portland']
query = indexer.fields['population'].range('0', '1')
# optimized to search the best field
assert str(query) == 'population[:1]:[0 TO 1}'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Portland']

query = indexer.fields['location'].prefix('CA:San')
# works like any prefix query
assert str(query) == 'state:city:CA:San*'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco']
query = indexer.fields['location'].prefix('CA')
# optimized to search the best field
assert str(query) == 'state:CA*'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Los Angeles']
