import unittest
import os, optparse
import tempfile, shutil
import itertools
import warnings
from datetime import date
import lucene
from lupyne import engine
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

class TestCase(BaseTest):
    
    def testInterface(self):
        "Indexer and document interfaces."
        assert engine.IndexWriter.__init__.im_func.func_defaults[-1] == lucene.IndexWriter.DEFAULT_MAX_FIELD_LENGTH
        with warnings.catch_warnings(record=True) as deprecations:
            writer = engine.IndexWriter(analyzer=lucene.StandardAnalyzer)
            engine.IndexSearcher(writer.directory, analyzer=lucene.WhitespaceAnalyzer)
        assert len(deprecations) == 2
        Stemmer = engine.Analyzer(lucene.StandardAnalyzer(), lucene.PorterStemFilter, lucene.TypeAsPayloadTokenFilter)
        indexer = engine.Indexer(analyzer=Stemmer)
        self.assertRaises(lucene.JavaError, engine.Indexer, indexer.directory)
        indexer.set('text')
        indexer.set('name', store=True, index=False, boost=2.0)
        for field in indexer.fields['name'].items('sample'):
            assert isinstance(field, lucene.Field) and field.boost == 2.0
        indexer.set('tag', store=True, index=True)
        indexer.add(text='hello worlds', name='sample', tag=['python', 'search'])
        assert len(indexer) == 1 and list(indexer) == []
        assert not indexer.optimized
        indexer.commit()
        searcher = engine.ParallelMultiSearcher([indexer.indexSearcher, indexer.directory])
        assert searcher.count() == 2 * len(indexer)
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
        assert list(indexer.positions('text', 'world', payloads=True)) == [(0, [(1, '<ALPHANUM>')])]
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
        assert indexer.search('text:*hello', allowLeadingWildcard=True)
        indexer.delete('name:sample')
        indexer.delete('tag', 'python')
        assert 0 in indexer
        assert len(indexer) == sum(indexer.segments.values())
        indexer.commit()
        assert len(indexer) < sum(indexer.segments.values())
        indexer.optimize()
        assert len(indexer) == sum(indexer.segments.values())
        assert 0 not in indexer
        temp = engine.Indexer(self.tempdir)
        temp.add()
        temp.commit()
        indexer += temp
        indexer += temp.directory
        indexer += self.tempdir
        assert len(indexer) == 3
        x, y = engine.Filter([0, 1]).bits(), engine.Filter([1, 2]).bits()
        assert list(x & y) == [1] and list(x | y) == [0, 1, 2] and list(x -y) == [0]
        indexer.add(text=lucene.WhitespaceTokenizer(lucene.StringReader('?')))
        indexer.commit()
        assert list(indexer.terms('text')) == ['?']
    
    def testBasic(self):
        "Text fields and simple searches."
        self.assertRaises(lucene.JavaError, engine.Indexer, self.tempdir, 'r')
        indexer = engine.Indexer(self.tempdir)
        for name, params in fixture.constitution.fields.items():
            indexer.set(name, **params)
        for doc in fixture.constitution.docs():
            indexer.add(doc)
        indexer.commit()
        assert len(indexer) == len(indexer.search()) == 35
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
        comparator = map(int, indexer.comparator('amendment', default='0'))
        hits = indexer.search('text:people', sort=comparator.__getitem__)
        assert sorted(hits.ids) == hits.ids and hits.ids != ids
        comparator = indexer.comparator('article', 'amendment')
        hits = indexer.search('text:people', sort=comparator.__getitem__)
        assert sorted(hits.ids) != hits.ids
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
        query.boost = 2.0
        assert query.boost == 2.0
        hits = indexer.search(query)
        assert sorted(hit['amendment'] for hit in hits) == amendments
        hits = indexer.search(query | engine.Query.term('text', 'vote'))
        assert set(hit.get('amendment') for hit in hits) > set(amendments)
        hit, = indexer.search(query & engine.Query.term('text', 'vote'))
        assert hit['amendment'] == '19'
        f = engine.Filter(indexer.docs('text', 'vote'))
        for n in range(2):  # filters should be reusable
            hit, = indexer.search(query, filter=f)
            assert hit['amendment'] == '19'
        hit, = indexer.search(query - engine.Query.all(text='vote'))
        assert hit['amendment'] == '18'
        hit, = indexer.search(engine.Query.all(text=['persons', 'papers']))
        assert hit['amendment'] == '4'
        hit, = indexer.search(engine.Query.phrase('text', 'persons', None, 'papers'))
        assert hit['amendment'] == '4'
        hit, = indexer.search(engine.Query.multiphrase('text', 'persons', ['houses', 'papers']))
        assert hit['amendment'] == '4'
        query = engine.Query.term('text', 'persons')
        assert str(-query) == '-text:persons'
        query = +query
        query -= engine.Query.term('text', 'papers')
        assert str(query[-1]) == '-text:papers'
        assert len(query) == len(list(query)) == 2
        queries = [engine.Query.span('text', word) for word in ('persons', 'papers', 'things')]
        count = indexer.count(queries[0])
        near = queries[0].near(queries[1], slop=1)
        assert indexer.count(queries[0] - near) == count
        near = queries[0].near(queries[1] | queries[2], slop=1)
        assert indexer.count(queries[0] - near) == count - 1
        assert indexer.count(queries[0][:100]) == count - 1
        spans = dict(indexer.spans(queries[0]))
        assert len(spans) == count and spans == dict(indexer.docs('text', 'persons', counts=True))
        near = queries[0].near(queries[1], slop=2)
        (id, positions), = indexer.spans(near, positions=True)
        assert indexer[id]['amendment'] == '4' and positions == [(3, 6)]
        with warnings.catch_warnings(record=True) as leaks:
            assert 'persons' in indexer.termvector(id, 'text')
            assert dict(indexer.termvector(id, 'text', counts=True))['persons'] == 2
            assert dict(indexer.positionvector(id, 'text'))['persons'] == [3, 26]
            assert dict(indexer.positionvector(id, 'text', offsets=True))['persons'] == [(46, 53), (301, 308)]
        assert len(leaks) == 2 * (lucene.VERSION <= '2.4.1')
        del indexer
        assert engine.Indexer(self.tempdir)
    
    def testAdvanced(self):
        "Large data set with hierarchical fields."
        indexer = engine.Indexer(self.tempdir)
        for name, params in fixture.zipcodes.fields.items():
            indexer.set(name, **params)
        indexer.set('longitude', engine.PrefixField, store=True)
        indexer.fields['location'] = engine.NestedField('state:county:city')
        for doc in fixture.zipcodes.docs():
            if doc['state'] in ('CA', 'AK', 'WY', 'PR'):
                lat, lng = ('{0:08.3f}'.format(doc.pop(l)) for l in ['latitude', 'longitude'])
                location = ':'.join(doc[name] for name in ['state', 'county', 'city'])
                indexer.add(doc, latitude=lat, longitude=lng, location=location)
        indexer.commit()
        assert set(['state', 'zipcode']) < set(indexer.names('indexed'))
        assert set(['latitude', 'longitude', 'county', 'city']) == set(indexer.names('unindexed'))
        longitude = max(name for name in indexer.names() if name.startswith('longitude'))
        lngs = list(indexer.terms(longitude))
        east, west = lngs[0], lngs[-1]
        hit, = indexer.search(engine.Query.term(longitude, west))
        assert hit['state'] == 'AK' and hit['county'] == 'Aleutians West'
        hit, = indexer.search(engine.Query.term(longitude, east))
        assert hit['state'] == 'PR' and hit['county'] == 'Culebra'
        states = list(indexer.terms('state'))
        assert states[0] == 'AK' and states[-1] == 'WY'
        counties = [term.split(':')[-1] for term in indexer.terms('state:county', 'CA', 'CA~')]
        field = indexer.fields['location']
        hits = indexer.search(field.prefix('CA'))
        assert sorted(set(hit['county'] for hit in hits)) == counties
        assert counties[0] == 'Alameda' and counties[-1] == 'Yuba'
        cities = [term.split(':')[-1] for term in indexer.terms('state:county:city', 'CA:Los Angeles', 'CA:Los Angeles~')]
        hits = indexer.search(field.prefix('CA:Los Angeles'))
        assert sorted(set(hit['city'] for hit in hits)) == cities
        assert cities[0] == 'Acton' and cities[-1] == 'Woodland Hills'
        hit, = indexer.search('zipcode:90210')
        assert hit['state'] == 'CA' and hit['county'] == 'Los Angeles' and hit['city'] == 'Beverly Hills'
        assert hit['longitude'] == '-118.406'
        lng = hit['longitude'][:4]
        field = indexer.fields['longitude']
        hits = indexer.search(field.prefix(lng))
        assert hit.id in hits.ids
        assert len(hits) == indexer.count(engine.Query.prefix(longitude, lng))
        count = indexer.count(field.prefix(lng[:3]))
        assert count > len(hits)
        assert count == indexer.count(field.range(lng[:3], lng[:3]+'~'))
        assert count > indexer.count(engine.Query.term('state', 'CA'), filter=engine.Query.term(longitude, lng).filter())
        hits = indexer.search('zipcode:90*')
        (field, facets), = indexer.facets(hits.ids, 'state:county').items()
        assert field == 'state:county'
        la, orange = sorted(filter(facets.get, facets))
        assert la == 'CA:Los Angeles' and facets[la] > 100
        assert orange == 'CA:Orange' and facets[orange] > 10
    
    def testSpatial(self):
        "Spatial tile test."
        indexer = engine.Indexer(self.tempdir)
        for name, params in fixture.zipcodes.fields.items():
            indexer.set(name, **params)
        field = indexer.fields['tile'] = engine.PointField('tile', precision=15, start=10, step=2, store=True)
        for doc in fixture.zipcodes.docs():
            if doc['state'] == 'CA':
                lat, lng = doc.pop('latitude'), doc.pop('longitude')
                indexer.add(doc, tile=[(lng, lat)], latitude=str(lat), longitude=str(lng))
        indexer.commit()
        city, zipcode, tile = 'Beverly Hills', '90210', '023012311120332'
        hit, = indexer.search('zipcode:' + zipcode)
        assert hit['tile'] == tile and hit['city'] == city
        hit, = indexer.search(field.prefix(tile))
        assert hit['zipcode'] == zipcode and hit['city'] == city
        x, y = (float(hit[l]) for l in ['longitude', 'latitude'])
        assert field.coords(tile[:4]) == (2, 9)
        bottom, left, top, right = field.decode(tile)
        assert left < x < right and bottom < y < top
        hits = indexer.search(field.near(x, y))
        cities = set(hit['city'] for hit in hits)
        assert set([city]) == cities
        hits = indexer.search(field.near(x, y, precision=10))
        cities = set(hit['city'] for hit in hits)
        assert city in cities and len(cities) > 10
        hits = indexer.search(field.within(x, y, 10**4))
        cities = set(hit['city'] for hit in hits)
        assert city in cities and 100 > len(cities) > 50
        hits = indexer.search(field.within(x, y, 10**5))
        cities = set(hit['city'] for hit in hits)
        assert city in cities and len(cities) > 100
    
    def testFields(self):
        "Custom field tests."
        indexer = engine.Indexer(self.tempdir)
        indexer.set('amendment', engine.FormatField, format='{0:02n}', store=True)
        indexer.set('date', engine.DateTimeField, store=True)
        for doc in fixture.constitution.docs():
            if 'amendment' in doc:
                indexer.add(amendment=int(doc['amendment']), date=doc['date'])
        indexer.commit()
        query = engine.Query.range('amendment', '', indexer.fields['amendment'].format(10))
        assert indexer.count(query) == 9
        query = engine.Query.prefix('amendment', '0')
        assert indexer.count(query) == 9
        query = indexer.fields['date'].range('', '1921-12', lower=False, upper=True)
        assert str(query) == 'date:Y:{ TO 1921} date:Ym:[1921 TO 1921-12]'
        assert indexer.count(query) == 19
        query = indexer.fields['date'].range(date(1919, 1, 1), date(1921, 12, 31))
        cls = lucene.TermRangeQuery if hasattr(lucene, 'TermRangeQuery') else lucene.ConstantScoreRangeQuery
        fields = [cls.cast_(clause.query).field for clause in query]
        assert fields == ['date:Ymd', 'date:Ym', 'date:Y', 'date:Ym', 'date:Ymd']
        hits = indexer.search(query)
        assert [hit['amendment'] for hit in hits] == ['18', '19']
        assert [hit['date'].split('-')[0] for hit in hits] == ['1919', '1920']
        query = indexer.fields['date'].within(seconds=100)
        assert indexer.count(query) == 0
        query = indexer.fields['date'].within(-100*365)
        assert 0 < len(query) <= 5
        assert 0 < indexer.count(query) <= 12
        assert len(indexer.fields['date'].within(-100)) <= 3
        assert len(indexer.fields['date'].within(-100.0)) > 3
        assert len(indexer.fields['date'].within(-100, seconds=1, utc=True)) > 3

    def testHighlighting(self):
        "Highlighting text fragments."
        indexer = engine.Indexer()
        amendments = dict((doc['amendment'], doc['text']) for doc in fixture.constitution.docs() if 'amendment' in doc)
        for amendment in amendments.values():
            fragments = indexer.highlight('persons', amendment)
            assert len(fragments) == ('persons' in amendment)
            for fragment in fragments:
                assert '<B>persons</B>' in fragment
        text = amendments['4']
        query = '"persons, houses, papers"'
        fragments = indexer.highlight(query, text, count=3, span=False, formatter=lucene.SimpleHTMLFormatter('*', '*'))
        assert len(fragments) == 2 and fragments[0].count('*') == 2*3 and '*persons*' in fragments[1]
        fragment, = indexer.highlight(query, text, count=3, span=False, textFragmenter=lucene.SimpleFragmenter(200))
        assert len(fragment) > len(text) and fragment.count('<B>persons</B>') == 2
        fragment, = indexer.highlight(query, text, count=3)
        assert len(fragment) < len(text) and fragment.count('<B>') == 3

if __name__ == '__main__':
    lucene.initVM(lucene.CLASSPATH)
    unittest.main()
