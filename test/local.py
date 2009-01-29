import unittest, os, optparse
import tempfile, shutil
import itertools
import lucene
lucene.initVM(lucene.CLASSPATH)
import engine
import fixture

class BaseTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(dir=os.path.dirname(__file__))
        parser = optparse.OptionParser()
        parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
        options, args = parser.parse_args()
        self.verbose = options.verbose
    def tearDown(self):
        shutil.rmtree(self.tempdir)

class LocalTest(BaseTest):
    
    def test0Interface(self):
        "Indexer and document interfaces."
        indexer = engine.Indexer()
        self.assertRaises(lucene.JavaError, engine.Indexer, indexer.directory)
        searcher = engine.Indexer(indexer.directory, mode='r')
        assert not hasattr(searcher, 'commit')
        assert len(searcher) == 0 and list(searcher) == []
        indexer.set('text')
        indexer.set('name', store=True, index=False)
        indexer.set('tag', store=True, index=True)
        indexer.add(text='hello world', name='sample', tag=['python', 'search'])
        assert len(indexer) == 1 and list(indexer) == []
        assert not indexer.optimized
        indexer.commit()
        assert list(indexer) == [0]
        assert indexer.current and indexer.optimized
        assert 0 in indexer and 1 not in indexer
        doc = indexer[0]
        assert len(doc) == 3
        assert 'name' in doc and 'missing' not in doc
        assert sorted(doc.items()) == [('name', 'sample'), ('tag', 'python'), ('tag', 'search')]
        assert doc.dict('tag') == {'name': 'sample', 'tag': ['python', 'search']}
        assert doc.dict(name=None, missing=True) == {'name': 'sample', 'missing': True}
        doc['name'] == 'sample' and doc['tag'] in ('python', 'search')
        self.assertRaises(KeyError, doc.__getitem__, 'key')
        assert doc.getlist('name') == ['sample'] and doc.getlist('key') == []
        assert indexer.count('text', 'hello') == indexer.count('text:hello') == 1
        assert sorted(indexer.names()) == ['name', 'tag', 'text']
        assert sorted(indexer.names('indexed')) == ['tag', 'text']
        assert indexer.names('unindexed') == ['name']
        assert list(indexer.terms('text')) == ['hello', 'world']
        assert list(indexer.terms('text', 'h', 'v')) == ['hello']
        assert dict(indexer.terms('text', 'w', counts=True)) == {'world': 1}
        assert list(indexer.terms('test')) == []
        assert list(indexer.docs('text', 'hello')) == [0]
        assert list(indexer.docs('text', 'hi')) == []
        assert list(indexer.docs('text', 'world', counts=True)) == [(0, 1)]
        assert list(indexer.positions('text', 'world')) == [(0, [1])]
        hits = indexer.search('text:hello')
        assert len(hits) == hits.count == 1
        assert hits.ids == [0]
        score, = hits.scores
        assert 0 < score < 1
        assert dict(hits.items()) == {0: score}
        data = hits[0].dict()
        assert data['__id__'] == 0 and '__score__' in data
        assert not indexer.search('hello') and indexer.search('hello', field='text')
        assert indexer.search('text:hello hi') and not indexer.search('text:hello hi', op='and')
        indexer.delete('name:sample')
        indexer.delete('tag', 'python')
        assert 0 in indexer
        assert len(indexer) == sum(indexer.segments.values())
        indexer.commit()
        assert len(indexer) < sum(indexer.segments.values())
        indexer.optimize()
        assert len(indexer) == sum(indexer.segments.values())
        assert 0 not in indexer
        temp = engine.Indexer()
        temp.add()
        temp.commit()
        indexer += temp.directory
        assert len(indexer) == 1
    
    def test1Basic(self):
        "Text fields and simple searches."
        self.assertRaises(lucene.JavaError, engine.Indexer, self.tempdir, 'r')
        indexer = engine.Indexer(self.tempdir)
        for name, params in fixture.constitution.fields.items():
            indexer.set(name, **params)
        for doc in fixture.constitution.docs():
            indexer.add(doc)
        indexer.commit()
        assert len(indexer)== 35
        assert sorted(indexer.names()) == ['amendment', 'article', 'date', 'text']
        articles = list(indexer.terms('article'))
        articles.remove('Preamble')
        assert sorted(map(int, articles)) == range(1, 8)
        assert sorted(map(int, indexer.terms('amendment'))) == range(1, 28)
        assert list(itertools.islice(indexer.terms('text', 'right'), 2)) == ['right', 'rights']
        word, count = next(indexer.terms('text', 'people', counts=True))
        assert word == 'people' and count == 8
        docs = dict(indexer.docs('text', 'people', counts=True))
        counts = docs.values()
        assert len(docs) == count and all(counts) and sum(counts) > count
        positions = dict(indexer.positions('text', 'people'))
        assert map(len, positions.values()) == counts
        hit, = indexer.search('"We the People"', field='text')
        assert hit['article'] == 'Preamble'
        assert sorted(hit.dict()) == ['__id__', '__score__', 'article']
        hits = indexer.search('people', field='text')
        assert len(hits) == hits.count == 8
        ids = hits.ids
        hits = indexer.search('people', count=5, field='text')
        assert hits.ids == ids[:len(hits)]
        assert len(hits) == 5 and hits.count == 8
        hits = indexer.search('text:people', count=5, sort='amendment')
        assert sorted(hits.ids) == hits.ids and hits.ids != ids[:len(hits)]
        hits = indexer.search('text:people', count=5, sort='amendment', reverse=True)
        assert sorted(hits.ids, reverse=True) == hits.ids and hits.ids != ids[:len(hits)]
        hit, = indexer.search('freedom', field='text')
        assert hit['amendment'] == '1'
        assert sorted(hit.dict()) == ['__id__', '__score__', 'amendment', 'date']
        hits = indexer.search('text:right')
        hits = indexer.search('text:people', filter=hits.ids)
        assert len(hits) == 4
        hit, = indexer.search('date:192*')
        assert hit['amendment'] == '19'
        hits = indexer.search('date:[1919 TO 1921]')
        amendments = ['18', '19']
        assert sorted(hit['amendment'] for hit in hits) == amendments
        query = engine.Query.range('date', '1919', '1921')
        hits = indexer.search(query)
        assert sorted(hit['amendment'] for hit in hits) == amendments
        hits = indexer.search(query | engine.Query.term('text', 'vote'))
        assert set(hit.get('amendment') for hit in hits) > set(amendments)
        hit, = indexer.search(query & engine.Query.term('text', 'vote'))
        assert hit['amendment'] == '19'
        hit, = indexer.search(query - engine.Query.term('text', 'vote'))
        assert hit['amendment'] == '18'
        del indexer
        assert engine.Indexer(self.tempdir)

    def test2Advanced(self):
        "Large data set with hierarchical fields."
        indexer = engine.Indexer(self.tempdir)
        for name, params in fixture.zipcodes.fields.items():
            indexer.set(name, **params)
        indexer.fields['location'] = engine.NestedField('location')
        indexer.fields['longitude'] = engine.PrefixField('longitude', store=True)
        for doc in fixture.zipcodes.docs():
            if doc['state'] in ('CA', 'AK', 'WY', 'PR'):
                lat, lng = ('%08.3f' % doc.pop(l) for l in ['latitude', 'longitude'])
                location = ':'.join(doc[name] for name in ['state', 'county', 'city'])
                indexer.add(doc, latitude=lat, longitude=lng, location=location)
        indexer.commit()
        assert set(['location', 'longitude', 'zipcode']) <= set(indexer.names('indexed'))
        assert set(['city', 'county', 'state']) <= set(indexer.names('unindexed'))
        lngs = list(indexer.terms('longitude'))
        east, west = lngs[0], lngs[-1]
        hit, = indexer.search(engine.Query.term('longitude', west))
        assert hit['state'] == 'AK' and hit['county'] == 'Aleutians West'
        hit, = indexer.search(engine.Query.term('longitude', east))
        assert hit['state'] == 'PR' and hit['county'] == 'Culebra'
        states = list(indexer.terms('location'))
        assert states[0] == 'AK' and states[-1] == 'WY'
        counties = list(indexer.terms('location:CA'))
        field = indexer.fields['location']
        hits = indexer.search(field.query('CA'))
        assert sorted(set(hit['county'] for hit in hits)) == counties
        assert counties[0] == 'Alameda' and counties[-1] == 'Yuba'
        cities = list(indexer.terms('location:CA:Los Angeles'))
        hits = indexer.search(field.query('CA:Los Angeles'))
        assert sorted(set(hit['city'] for hit in hits)) == cities
        assert cities[0] == 'Acton' and cities[-1] == 'Woodland Hills'
        hit, = indexer.search('zipcode:90210')
        assert hit['state'] == 'CA' and hit['county'] == 'Los Angeles' and hit['city'] == 'Beverly Hills'
        assert hit['longitude'] == '-118.406'
        lng = hit['longitude'][:4]
        field = indexer.fields['longitude']
        hits = indexer.search(field.query(lng))
        assert hit.id in hits.ids
        assert len(hits) == indexer.count(engine.Query.prefix('longitude', lng))
        count = indexer.count(field.query(lng[:3]))
        assert count > len(hits)
        self.assertRaises(lucene.JavaError, indexer.search, engine.Query.prefix('longitude', lng[:3]))
        assert count > indexer.count(engine.Query.term('location', 'CA'), filter=lucene.PrefixFilter(lucene.Term('longitude', lng)))

    def test3Spatial(self):
        "Optional spatial test."
        try:
            from engine import spatial
        except ImportError:
            if self.verbose:
                print 'Geohash not installed;  skipping spatial test.'
            return
        indexer = engine.Indexer(self.tempdir)
        for name, params in fixture.zipcodes.fields.items():
            indexer.set(name, **params)
        indexer.fields['location'] = spatial.PointField('location', precision=6, store=True)
        for doc in fixture.zipcodes.docs():
            if doc['state'] == 'CA':
                lat, lng = doc.pop('latitude'), doc.pop('longitude')
                indexer.add(doc, location=[(lng, lat)], latitude=str(lat), longitude=str(lng))
        indexer.commit()
        field = indexer.fields['location']
        city, zipcode, location = 'Beverly Hills', '90210', '9q5cct'
        hit, = indexer.search('zipcode:' + zipcode)
        assert hit['location'] == location and hit['city'] == city
        hit, = indexer.search('location:' + location)
        assert hit['zipcode'] == zipcode and hit['city'] == city
        x, y = (float(hit[l]) for l in ['longitude', 'latitude'])
        hits = indexer.search(field.query(x, y, precision=5))
        cities = set(hit['city'] for hit in hits)
        assert set([city]) == cities
        hits = indexer.search(field.query(x, y, precision=4))
        cities = set(hit['city'] for hit in hits)
        assert city in cities and len(cities) > 10
        hits = indexer.search(field.within(x, y, 0.1))
        cities = set(hit['city'] for hit in hits)
        assert city in cities and len(cities) > 50

if __name__ == '__main__':
    unittest.main()
