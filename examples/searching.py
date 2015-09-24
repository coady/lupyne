"""
Advanced searching with custom fields.

Prefix and Range queries are a potential pitfall in Lucene.
As the queries expand to more terms, the performance drops off precipitously.
A common example is where datetimes are indexed, but a large span of date ranges are being searched.
The usual workaround is to only index the amount of granularity needed, e.g., just the dates.
But this may not be sufficient, or the datetimes may be necessary for other searches.

The general solution is to index the term values into a prefix tree.
Then each query can expand to only values of the appropriate granularity.
Lucene's NumericFields encode numbers to be sortable, so it is also able to cluster prefixes into the same field.
Whereas Lupyne's NestedField assumes the value is already a sortable string, so different fields must be used to cluster the prefixes.
There are trade-offs to each approach:
 * NumericFields support range queries natively, but must translate prefix queries.
 * NestedFields support prefix queries optimally, but must translate range queries.
 * NumericFields only support numbers, and result in unreadable values in the index.
 * NestedFields support any searchable values, but pollute the field namespace.

Lupyne PointFields and DateTimeFields are implemented as NumericFields since both are easily encoded as numbers.
NestedFields could still be used however, as demonstrated on dates below.
"""

from datetime import date
import lucene
from org.apache.lucene import search
from lupyne import engine
lucene.initVM()

docs = [
    {'city': 'San Francisco', 'state': 'CA', 'incorporated': '1850-04-15', 'population': 808976, 'longitude': -122.4192, 'latitude': 37.7752},
    {'city': 'Los Angeles', 'state': 'CA', 'incorporated': '1850-04-04', 'population': 3849378, 'longitude': -118.2434, 'latitude': 34.0521},
    {'city': 'Portland', 'state': 'OR', 'incorporated': '1851-02-08', 'population': 575930, 'longitude': -122.6703, 'latitude': 45.5238},
]

indexer = engine.Indexer()
indexer.set('city', stored=True, indexed=False)
indexer.set('state', stored=True, indexed=False)
# set method supports custom field types inheriting their default settings
indexer.set('incorporated', engine.DateTimeField)
indexer.set('year-month-day', engine.NestedField, sep='-')
indexer.set('population', engine.NumericField, type=int)
indexer.set('point', engine.PointField, precision=10)
# assigned fields can have a different key from their underlying field name
indexer.fields['location'] = engine.NestedField('state.city')

for doc in docs:
    doc['year-month-day'] = doc['incorporated']
    point = doc.pop('longitude'), doc.pop('latitude')
    location = doc['state'] + '.' + doc['city']
    incorporated = map(int, doc.pop('incorporated').split('-'))
    indexer.add(doc, location=location, incorporated=date(*incorporated), point=[point])
indexer.commit()

query = indexer.fields['incorporated'].prefix([1850])
assert query.max.doubleValue() - query.min.doubleValue() == 60 * 60 * 24 * 365
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Los Angeles']
query = indexer.fields['incorporated'].range(date(1850, 4, 10), None)
assert query.max is None
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Portland']

query = indexer.fields['year-month-day'].prefix('1850')
assert str(query) == 'year:1850*'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Los Angeles']
query = indexer.fields['year-month-day'].range('1850-04-10', None)
assert str(query) == 'year-month-day:[1850-04-10 TO *}'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Portland']

query = indexer.fields['population'].range(0, 1000000)
assert str(query) == 'population:[0 TO 1000000}'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Portland']

cities = ['San Francisco', 'Los Angeles', 'Portland']
for index, distance in enumerate([1e3, 1e5, 2e5, 1e6]):
    query = indexer.fields['point'].within(-122.4, 37.7, distance=distance)
    assert isinstance(query, search.BooleanQuery) and len(query) <= 4
    assert {hit['city'] for hit in indexer.search(query)} == set(cities[:index])

query = indexer.fields['location'].prefix('CA.San')
# works like any prefix query
assert str(query) == 'state.city:CA.San*'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco']
query = indexer.fields['location'].prefix('CA')
# optimized to search the best field
assert str(query) == 'state:CA*'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Los Angeles']
