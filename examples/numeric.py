"""
Numeric fields.

Lupyne's PrefixField was created before Lucene's NumericField was added in version 2.9.
Both support indexing a prefix tree of values, in order to optimize range and prefix queries, but use different approaches.

NumericFields encode numbers to be sortable, so it is also able to cluster prefixes into the same field.
Whereas PrefixField assumes the value is already a sortable string, so different fields must be used to cluster the prefixes.
There are trade-offs to each approach:
 * NumericFields support range queries natively, but must translate prefix queries.
 * PrefixFields support prefix queries optimally, but must translate range queries.
 * NumericFields only support numbers, and result in unreadable values in the index.
 * PrefixFields support any searchable values, but pollute the field namespace.

Spatial and datetime fields are two common examples that need prefix tree support.
Currently SpatialFields and DateTimeFields are based on PrefixFields, but have an alternate NumericField implementation.
Because both are easily encodable as numbers, the plan is to make the numeric implementation the default when support for 2.4 is dropped.

So the long term support for PrefixField is unclear, although sometimes it is convenient to index into different fields.
For example, breaking datetimes into their components makes searching by year optimal, and it's easier to introspect the index.
There will be continued support for NestedFields, which allow arbitrary compound indexes similar to a relational database.
See the state:city field in the searching example.
"""

from datetime import date, datetime
import lucene
lucene.initVM(lucene.CLASSPATH)
from lupyne import engine

docs = [
    {'city': 'San Francisco', 'incorporated': '1850-04-15', 'population': 808976, 'longitude': -122.4192, 'latitude': 37.7752},
    {'city': 'Los Angeles', 'incorporated': '1850-04-04', 'population': 3849378, 'longitude': -118.2434, 'latitude': 34.0521},
    {'city': 'Portland', 'incorporated': '1851-02-08', 'population': 575930, 'longitude': -122.6703, 'latitude': 45.5238},
]

indexer = engine.Indexer()
indexer.set('city', store=True, index=False)
indexer.set('incorporated', engine.numeric.DateTimeField)
indexer.set('population', engine.numeric.NumericField)
indexer.set('point', engine.numeric.PointField, precision=10)

for doc in docs:
    point = doc.pop('longitude'), doc.pop('latitude')
    incorporated = map(int, doc.pop('incorporated').split('-'))
    indexer.add(doc, incorporated=date(*incorporated), point=[point])
indexer.commit()

query = indexer.fields['incorporated'].prefix([1850])
assert query.max.doubleValue() - query.min.doubleValue() == 60 * 60 * 24 * 365
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Los Angeles']
query = indexer.fields['incorporated'].range(date(1850, 4, 10), None)
assert query.max is None
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Portland']

query = indexer.fields['population'].range(0, 1000000)
assert str(query) == 'population:[0 TO 1000000}'
assert [hit['city'] for hit in indexer.search(query)] == ['San Francisco', 'Portland']

cities = ['San Francisco', 'Los Angeles', 'Portland']
for index, distance in enumerate([1e3, 1e5, 2e5, 1e6]):
    query = indexer.fields['point'].within(-122.4, 37.7, distance=distance)
    assert isinstance(query, lucene.BooleanQuery) and len(query) <= 4
    assert set(hit['city'] for hit in indexer.search(query)) == set(cities[:index])
