from future_builtins import map, zip
import unittest
import os
import tempfile, shutil
import itertools
import collections
import warnings
import datetime
import math
from contextlib import contextmanager
import lucene
from lupyne import engine
import fixture

class typeAsPayload(engine.TokenFilter):
    "Custom implementation of lucene TypeAsPayloadTokenFilter."
    def incrementToken(self):
        result = engine.TokenFilter.incrementToken(self)
        self.payload = self.type.encode('utf8')
        return result

@contextmanager
def assertWarns(*categories):
    with warnings.catch_warnings(record=True) as messages:
        yield
    assert len(messages) == len(categories), messages
    for message, category in zip(messages, categories):
         assert issubclass(message.category, category), message

class Filter(lucene.PythonFilter):
    "Broken filter to test errors are raised."
    def getBitSet(self, indexReader):
        assert False
    getDocIdSet = getBitSet

class BaseTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(dir=os.path.dirname(__file__))
    def tearDown(self):
        shutil.rmtree(self.tempdir)

class TestCase(BaseTest):
    
    def testInterface(self):
        "Indexer and document interfaces."
        self.assertRaises(TypeError, engine.IndexSearcher)
        analyzer = lucene.StandardAnalyzer(lucene.Version.values()[-1])
        stemmer = engine.Analyzer(analyzer, lucene.PorterStemFilter, typeAsPayload)
        for token in stemmer.tokens('hello'):
            assert token.positionIncrement == 1
            assert engine.TokenFilter(lucene.EmptyTokenStream()).payload is None
            assert token.term == 'hello'
            assert token.type == token.payload == '<ALPHANUM>'
            assert token.offset == (0, 5)
            token.term = token.type = ''
            token.offset, token.positionIncrement = (0, 0), 0
        assert str(stemmer.parse('hellos', field=['body', 'title'])) == 'body:hello title:hello'
        assert str(stemmer.parse('hellos', field={'body': 1.0, 'title': 2.0})) == 'body:hello title:hello^2.0'
        indexer = engine.Indexer(analyzer=stemmer, version=lucene.Version.LUCENE_30)
        self.assertRaises(lucene.JavaError, engine.Indexer, indexer.directory)
        indexer.set('text')
        indexer.set('name', store=True, index=False, boost=2.0)
        for field in indexer.fields['name'].items('sample'):
            assert isinstance(field, lucene.Field) and field.boost == 2.0
        indexer.set('tag', store=True, index=True)
        searcher = indexer.indexSearcher
        indexer.commit()
        assert searcher is indexer.indexSearcher
        indexer.add(text='hello worlds', name='sample', tag=['python', 'search'])
        assert len(indexer) == 1 and list(indexer) == []
        assert not indexer.optimized
        indexer.commit()
        assert searcher is not indexer.indexSearcher
        with assertWarns(DeprecationWarning, DeprecationWarning):
            searcher = engine.ParallelMultiSearcher([indexer.indexSearcher, indexer.directory])
        assert searcher.count() == 2 * len(indexer)
        assert list(indexer) == [0]
        assert indexer.current and indexer.optimized
        assert 0 in indexer and 1 not in indexer
        doc = indexer[0]
        assert len(doc) == 3
        assert 'name' in doc and 'missing' not in doc
        assert sorted(doc) == ['name', 'tag', 'tag']
        assert sorted(doc.items()) == [('name', 'sample'), ('tag', 'python'), ('tag', 'search')]
        assert doc.dict('tag') == {'name': 'sample', 'tag': ['python', 'search']}
        assert doc.dict(name=None, missing=True) == {'name': 'sample', 'missing': True}
        doc['name'] == 'sample' and doc['tag'] in ('python', 'search')
        self.assertRaises(KeyError, doc.__getitem__, 'key')
        assert doc.getlist('name') == ['sample'] and doc.getlist('key') == []
        assert indexer.get(0, 'name').dict() == {'name': 'sample'}
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
        query = engine.Query.multiphrase('text', ('hello', 'hi'), None, 'world')
        assert str(query) == 'text:"(hello hi) world"' and list(query.positions) == [0, 2]
        query = engine.Query.wildcard('text', '*')
        assert str(query) == 'text:*' and isinstance(query, lucene.WildcardQuery)
        assert str(lucene.MatchAllDocsQuery() | query) == '*:* text:*'
        assert str(lucene.MatchAllDocsQuery() - query) == '*:* -text:*'
        query = +query
        query &= engine.Query.fuzzy('text', 'hello')
        query |= engine.Query.fuzzy('text', 'hello', 0.1)
        assert str(query) == '+text:* +text:hello~0.5 text:hello~0.1'
        query = engine.Query.span('text', 'world')
        assert str(query.mask('name')) == 'mask(text:world) as name'
        query = engine.Query.disjunct(0.1, query, name='sample')
        assert str(query) == '(text:world | name:sample)~0.1'
        query = engine.Query.near('text', 'hello', ('tag', 'python'), slop=-1, inOrder=False)
        assert str(query) == 'spanNear([text:hello, mask(tag:python) as text], -1, false)' and indexer.count(query) == 1
        query = engine.Query.near('text', 'hello', 'world')
        (doc, items), = indexer.spans(query, payloads=True)
        (start, stop, payloads), = items
        assert doc == 0 and start == 0 and stop == 2 and payloads == ['<ALPHANUM>', '<ALPHANUM>']
        query = engine.Query.near('text', 'hello', 'world', collectPayloads=False)
        (doc, items), = indexer.spans(query, payloads=True)
        assert doc == 0 and items == []
        indexer.delete('name:sample')
        indexer.delete('tag', 'python')
        assert 0 in indexer
        assert len(indexer) == sum(indexer.segments.values())
        indexer.commit()
        assert len(indexer) < sum(indexer.segments.values())
        indexer.optimize()
        assert len(indexer) == sum(indexer.segments.values())
        assert 0 not in indexer
        indexer.add(tag='test', name='old')
        indexer.update('tag', boost=2.0, tag='test')
        indexer.commit()
        assert [indexer[id].dict() for id in indexer] == [{'tag': 'test'}]
        indexer.update('tag', 'test', {'name': 'new'})
        indexer.commit()
        assert [indexer[id].dict() for id in indexer] == [{'name': 'new'}]
        indexer.deleteAll()
        indexer.commit()
        temp = engine.Indexer(self.tempdir)
        temp.add()
        temp.commit()
        indexer += temp
        indexer += temp.directory
        indexer += self.tempdir
        assert len(indexer) == 3
        indexer.add(text=lucene.WhitespaceTokenizer(lucene.StringReader('?')))
        indexer.commit()
        assert list(indexer.terms('text')) == ['?']
        reader = engine.indexers.IndexReader(indexer.indexReader)
        assert reader[0].dict() == {} and reader.count('text', '?') == 1
        assert len(reader.comparator('text')) == 4
        indexer.delete('text', '?')
        indexer.commit(expunge=True)
        assert not indexer.hasDeletions()
        indexer.commit(optimize=2)
        len(indexer.segments) <= 2
        indexer.commit(optimize=True)
        assert indexer.optimized
        del reader.indexReader
        self.assertRaises(AttributeError, getattr, reader, 'maxDoc')
        del indexer.indexSearcher
        self.assertRaises(AttributeError, getattr, indexer, 'search')
    
    def testBasic(self):
        "Text fields and simple searches."
        self.assertRaises(lucene.JavaError, engine.Indexer, self.tempdir, 'r')
        indexer = engine.Indexer(self.tempdir)
        for name, params in fixture.constitution.fields.items():
            indexer.set(name, **params)
        for doc in fixture.constitution.docs():
            indexer.add(doc, boost=('article' in doc) + 1.0)
        indexer.commit()
        searcher = engine.IndexSearcher.load(self.tempdir)
        engine.IndexSearcher.load(searcher.directory) # ensure directory isn't closed
        assert len(indexer) == len(searcher) and lucene.RAMDirectory.instance_(searcher.directory)
        assert indexer.filters == indexer.spellcheckers == {}
        assert indexer.facets(lucene.MatchAllDocsQuery(), 'amendment')
        assert indexer.suggest('amendment', '')
        assert list(indexer.filters) == list(indexer.spellcheckers) == ['amendment']
        indexer.delete('amendment', doc['amendment'])
        indexer.add(doc)
        indexer.commit(filters=True, spellcheckers=True)
        assert list(indexer.filters) == list(indexer.spellcheckers) == ['amendment']
        doc['amendment'] = engine.Analyzer(lucene.WhitespaceTokenizer).tokens(doc['amendment'])
        doc['date'] = engine.Analyzer(lucene.WhitespaceTokenizer).tokens(doc['date']), 2.0
        scores = list(searcher.match(doc, 'text:congress', 'text:law', 'amendment:27', 'date:19*'))
        assert 0.0 == scores[0] < scores[1] < scores[2] < scores[3] == 1.0
        searcher = engine.MultiSearcher([indexer.directory, self.tempdir])
        assert searcher.count() == len(searcher) == 2 * len(indexer)
        searcher = searcher.reopen()
        assert searcher.facets(lucene.MatchAllDocsQuery(), 'amendment')['amendment'] == dict.fromkeys(map(str, range(1, 28)), 2)
        reader = searcher.indexReader
        del searcher
        self.assertRaises(lucene.JavaError, reader.isCurrent)
        assert len(indexer) == len(indexer.search()) == 35
        assert sorted(indexer.names()) == ['amendment', 'article', 'date', 'text']
        articles = list(indexer.terms('article'))
        articles.remove('Preamble')
        assert sorted(map(int, articles)) == range(1, 8)
        assert sorted(map(int, indexer.terms('amendment'))) == range(1, 28)
        assert list(itertools.islice(indexer.terms('text', 'right'), 2)) == ['right', 'rights']
        assert list(indexer.terms('text', 'right*')) == ['right', 'rights']
        assert list(indexer.terms('text', 'right', minSimilarity=0.5)) == ['eight', 'right', 'rights']
        word, count = next(indexer.terms('text', 'people', counts=True))
        assert word == 'people' and count == 8
        docs = dict(indexer.docs('text', 'people', counts=True))
        counts = docs.values()
        assert len(docs) == count and all(counts) and sum(counts) > count
        positions = dict(indexer.positions('text', 'people'))
        assert list(map(len, positions.values())) == counts
        hit, = indexer.search('"We the People"', field='text')
        assert hit['article'] == 'Preamble'
        assert sorted(hit.dict()) == ['__id__', '__score__', 'article']
        hits = indexer.search('people', field='text')
        assert hits[0]['article'] == 'Preamble'
        assert len(hits) == hits.count == 8
        assert set(map(type, hits.ids)) == set([int]) and set(map(type, hits.scores)) == set([float])
        assert hits.maxscore == max(hits.scores)
        ids = hits.ids
        hits = indexer.search('people', count=5, field='text')
        assert hits.ids == ids[:len(hits)]
        assert len(hits) == 5 and hits.count == 8
        assert not any(map(math.isnan, hits.scores))
        assert hits.maxscore == max(hits.scores)
        hits = indexer.search('text:people', count=5, sort=lucene.Sort.INDEXORDER)
        assert sorted(hits.ids) == hits.ids
        sort = engine.SortField('amendment', type=int)
        hits = indexer.search('text:people', count=5, sort=sort)
        assert [hit.get('amendment') for hit in hits] == [None, None, '1', '2', '4']
        assert all(map(math.isnan, hits.scores))
        hits = indexer.search('text:right', count=10**7, sort=sort, scores=True)
        assert not any(map(math.isnan, hits.scores)) and sorted(hits.scores, reverse=True) != hits.scores
        assert math.isnan(hits.maxscore)
        hits = indexer.search('text:right', count=2, sort=sort, maxscore=True)
        assert hits.maxscore > max(hits.scores)
        comparator = indexer.comparator('amendment', type=int, parser=lambda value: int(value or -1))
        hits = indexer.search('text:people', sort=comparator.__getitem__)
        assert sorted(hits.ids) == hits.ids and hits.ids != ids
        comparator = list(zip(*map(indexer.comparator, ['article', 'amendment'])))
        hits = indexer.search('text:people', sort=comparator.__getitem__)
        assert sorted(hits.ids) != hits.ids
        hits = indexer.search('text:people', count=5, sort='amendment', reverse=True)
        assert [hit['amendment'] for hit in hits] == ['9', '4', '2', '17', '10']
        hit, = indexer.search('freedom', field='text')
        assert hit['amendment'] == '1'
        assert sorted(hit.dict()) == ['__id__', '__score__', 'amendment', 'date']
        hits = indexer.search('text:right')
        for name in ('amendment', 'article'):
            indexer.filters[name] = engine.Query.prefix(name, '').filter()
        query = engine.Query.term('text', 'right')
        assert indexer.facets(str(query), 'amendment', 'article') == {'amendment': 12, 'article': 1}
        self.assertRaises(lucene.InvalidArgsError, indexer.overlap, query.filter(), query.filter(cache=False))
        hits = indexer.search('text:people', filter=query.filter())
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
        assert set(query.terms()) == set([('text', 'persons'), ('text', 'papers')])
        assert str(query[-1]) == '-text:papers'
        assert len(query) == len(list(query)) == 2
        span = engine.Query.span('text', 'persons')
        count = indexer.count(span)
        near = engine.Query.near('text', 'persons', 'papers', slop=1, inOrder=False)
        assert indexer.count(span - near) == count
        near = span.near(engine.Query.span('text', 'papers') | engine.Query.span('text', 'things'), slop=1)
        assert indexer.count(span - near) == count - 1
        assert 0 < indexer.count(span[:100]) < count
        spans = dict(indexer.spans(span))
        assert len(spans) == count and spans == dict(indexer.docs('text', 'persons', counts=True))
        near = engine.Query.near('text', 'persons', 'papers', slop=2)
        (id, positions), = indexer.spans(near, positions=True)
        assert indexer[id]['amendment'] == '4' and positions in ([(3, 6)], [(10, 13)])
        assert 'persons' in indexer.termvector(id, 'text')
        assert dict(indexer.termvector(id, 'text', counts=True))['persons'] == 2
        assert dict(indexer.positionvector(id, 'text'))['persons'] in ([3, 26], [10, 48])
        assert dict(indexer.positionvector(id, 'text', offsets=True))['persons'] == [(46, 53), (301, 308)]
        query = indexer.morelikethis(0)
        assert str(query) == 'text:united text:states'
        hits = indexer.search(query & engine.Query.prefix('article', ''))
        assert len(hits) == 8 and hits[0]['article'] == 'Preamble'
        assert str(indexer.morelikethis(0, 'article')) == ''
        assert str(indexer.morelikethis(0, minDocFreq=3)) == 'text:establish text:united text:states'
        assert str(indexer.morelikethis('jury', 'text', minDocFreq=4, minTermFreq=1)) == 'text:jury'
        assert str(indexer.morelikethis('jury', 'article')) == ''
        self.assertRaises(lucene.JavaError, indexer.morelikethis, 'jury')
        assert indexer.suggest('missing', '') == list(indexer.correct('missing', '')) == []
        assert indexer.suggest('text', '')[:8] == ['shall', 'states', 'any', 'have', 'united', 'congress', 'state', 'constitution']
        assert indexer.suggest('text', 'con')[:2] == ['congress', 'constitution']
        assert indexer.suggest('text', 'congress') == ['congress']
        assert indexer.suggest('text', 'congresses') == []
        assert list(indexer.correct('text', 'writ', distance=0, minSimilarity=None)) == ['writ']
        assert list(indexer.correct('text', 'write', distance=0, minSimilarity=None)) == []
        assert list(indexer.correct('text', 'write', distance=0)) == ['crime', 'writs', 'written', 'writ']
        assert list(indexer.correct('text', 'write', distance=0, minSimilarity=0.7)) == ['writs', 'writ']
        assert list(indexer.correct('text', 'write', distance=1, minSimilarity=None)) == ['writs', 'writ']
        assert list(indexer.correct('text', 'write', distance=1)) == ['writs', 'writ', 'crime', 'written']
        assert list(indexer.correct('text', 'write', distance=1, minSimilarity=0.7)) == ['writs', 'writ']
        assert list(indexer.correct('text', 'write', minSimilarity=0.9)) == ['writs', 'writ', 'crime', 'written']
        query = indexer.parse('text:write', spellcheck=True)
        assert lucene.TermQuery.instance_(query) and str(query) == 'text:writs'
        query = indexer.parse('"hello world"', field='text', spellcheck=True)
        assert lucene.PhraseQuery.instance_(query) and str(query) == 'text:"held would"'
        assert str(indexer.parse('vwxyz', field='text', spellcheck=True)) == 'text:vwxyz'
        del indexer
        assert engine.Indexer(self.tempdir)
    
    def testAdvanced(self):
        "Large data set with hierarchical fields."
        indexer = engine.Indexer(self.tempdir)
        for name, params in fixture.zipcodes.fields.items():
            indexer.set(name, **params)
        indexer.fields['location'] = engine.NestedField('state.county.city')
        for doc in fixture.zipcodes.docs():
            if doc['state'] in ('CA', 'AK', 'WY', 'PR'):
                lat, lng = ('{0:08.3f}'.format(doc.pop(l)) for l in ['latitude', 'longitude'])
                location = '.'.join(doc[name] for name in ['state', 'county', 'city'])
                indexer.add(doc, latitude=lat, longitude=lng, location=location)
        indexer.commit()
        assert set(['state', 'zipcode']) < set(indexer.names('indexed'))
        assert set(['latitude', 'longitude', 'county', 'city']) == set(indexer.names('unindexed'))
        states = list(indexer.terms('state'))
        assert states[0] == 'AK' and states[-1] == 'WY'
        counties = [term.split('.')[-1] for term in indexer.terms('state.county', 'CA', 'CA~')]
        field = indexer.fields['location']
        hits = indexer.search(field.prefix('CA'))
        assert sorted(set(hit['county'] for hit in hits)) == counties
        assert counties[0] == 'Alameda' and counties[-1] == 'Yuba'
        cities = [term.split('.')[-1] for term in indexer.terms('state.county.city', 'CA.Los Angeles', 'CA.Los Angeles~')]
        hits = indexer.search(field.prefix('CA.Los Angeles'))
        assert sorted(set(hit['city'] for hit in hits)) == cities
        assert cities[0] == 'Acton' and cities[-1] == 'Woodland Hills'
        hit, = indexer.search('zipcode:90210')
        assert hit['state'] == 'CA' and hit['county'] == 'Los Angeles' and hit['city'] == 'Beverly Hills' and hit['longitude'] == '-118.406'
        query = engine.Query.prefix('zipcode', '90')
        (field, facets), = indexer.facets(query.filter(), 'state.county').items()
        assert field == 'state.county'
        la, orange = sorted(filter(facets.get, facets))
        assert la == 'CA.Los Angeles' and facets[la] > 100
        assert orange == 'CA.Orange' and facets[orange] > 10
        (field, facets), = indexer.facets(query, ('state.county', 'CA.*')).items()
        assert all(value.startswith('CA.') for value in facets) and set(facets) < set(indexer.filters['state.county'])
        for count in (None, len(indexer)):
            hits = indexer.search(query, count=count, timeout=0.01)
            assert 0 <= len(hits) <= indexer.count(query) and hits.count in (None, len(hits)) and hits.maxscore in (None, 1.0)
            hits = indexer.search(query, count=count, timeout=-1)
            assert len(hits) == 0 and hits.count is hits.maxscore is None
        self.assertRaises(lucene.JavaError, indexer.search, filter=Filter())
        directory = lucene.RAMDirectory()
        query = engine.Query.term('state', 'CA')
        size = indexer.copy(directory, query)
        searcher = engine.IndexSearcher(directory)
        assert len(searcher) == size and list(searcher.terms('state')) == ['CA']
        path = os.path.join(self.tempdir, 'temp')
        size = indexer.copy(path, exclude=query, optimize=True)
        assert len(searcher) + size == len(indexer)
        searcher = engine.IndexSearcher(path)
        assert searcher.optimized and 'CA' not in searcher.terms('state')
        directory.close()
    
    def testSpatial(self):
        "Spatial tiles."
        indexer = engine.Indexer(self.tempdir, 'w')
        for name, params in fixture.zipcodes.fields.items():
            indexer.set(name, **params)
        field = indexer.fields['tile'] = engine.PointField('tile', precision=15, step=2, store=True)
        points = []
        for doc in fixture.zipcodes.docs():
            if doc['state'] == 'CA':
                lat, lng = doc.pop('latitude'), doc.pop('longitude')
                indexer.add(doc, tile=[(lng, lat)], latitude=str(lat), longitude=str(lng))
                if doc['city'] == 'Los Angeles':
                    points.append((lng, lat))
        assert len(list(engine.PolygonField('', precision=15).items(points))) > len(points)
        indexer.commit()
        city, zipcode, tile = 'Beverly Hills', '90210', '023012311120332'
        hit, = indexer.search('zipcode:' + zipcode)
        assert (hit['tile'] == tile or int(hit['tile']) == int(tile, 4)) and hit['city'] == city
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
        query = field.within(x, y, 10**4)
        assert len(query) < 3
        cities = set(hit['city'] for hit in indexer.search(query))
        assert city in cities and 100 > len(cities) > 50
        hits = indexer.search(field.within(x, y, 10**5))
        cities = set(hit['city'] for hit in hits)
        assert city in cities and len(cities) > 100
        assert len(field.within(x, y, 10**8)) == 1
        del indexer
    
    def testFields(self):
        "Custom fields."
        indexer = engine.Indexer(self.tempdir)
        indexer.set('amendment', engine.FormatField, format='{0:02d}', store=True)
        indexer.set('size', engine.FormatField, format='{0:04d}', store=True)
        field = indexer.fields['date'] = engine.NestedField('Y-m-d', sep='-', store=True)
        for doc in fixture.constitution.docs():
            if 'amendment' in doc:
                indexer.add(amendment=int(doc['amendment']), date=doc['date'], size=len(doc['text']))
        indexer.commit()
        query = engine.Query.range('amendment', '', indexer.fields['amendment'].format(10))
        assert indexer.count(query) == 9
        query = engine.Query.prefix('amendment', '0')
        assert indexer.count(query) == 9
        query = field.prefix('1791-12-15')
        assert indexer.count(query) == 10
        query = field.range('', '1921-12', lower=False, upper=True)
        assert str(query) == 'Y-m:{ TO 1921-12]', query
        assert indexer.count(query) == 19
        query = field.range('1919-01-01', '1921-12-31')
        assert str(query) == 'Y-m-d:[1919-01-01 TO 1921-12-31}'
        hits = indexer.search(query)
        assert [hit['amendment'] for hit in hits] == ['18', '19']
        assert [hit['Y-m-d'].split('-')[0] for hit in hits] == ['1919', '1920']
        field = indexer.fields['size']
        sizes = dict((id, int(indexer[id]['size'])) for id in indexer)
        ids = sorted((id for id in sizes if sizes[id] >= 1000), key=sizes.get)
        query = engine.Query.range('size', '1000', None)
        hits = indexer.search(query, sort=sizes.get)
        assert hits.ids == ids
        hits = indexer.search(query, count=3, sort=engine.SortField('size', type=long))
        assert hits.ids == ids[:len(hits)]
        query = engine.Query.range('size', None, '1000')
        assert indexer.count(query) == len(sizes) - len(ids)
        indexer.sorters['year'] = engine.SortField('Y-m-d', type=int, parser=lambda date: int(date.split('-')[0]))
        assert indexer.comparator('year')[:10] == [1791] * 10
        hits = indexer.search(count=3, sort='year')
        assert [int(hit['amendment']) for hit in hits] == [1, 2, 3]
        cache = len(lucene.FieldCache.DEFAULT.cacheEntries)
        hits = indexer.search(count=3, sort='year', reverse=True)
        assert [int(hit['amendment']) for hit in hits] == [27, 26, 25]
        assert cache == len(lucene.FieldCache.DEFAULT.cacheEntries)
        indexer.add()
        indexer.commit(sorters=True)
        cache = len(lucene.FieldCache.DEFAULT.cacheEntries)
        assert indexer.comparator('year')[-1] == 0
        assert cache == len(lucene.FieldCache.DEFAULT.cacheEntries)
    
    def testNumeric(self):
        "Numeric fields."
        indexer = engine.Indexer(self.tempdir)
        indexer.set('amendment', engine.NumericField, store=True)
        indexer.set('date', engine.DateTimeField, store=True)
        indexer.set('size', engine.NumericField, store=True, step=5)
        for doc in fixture.constitution.docs():
            if 'amendment' in doc:
                indexer.add(amendment=int(doc['amendment']), date=[tuple(map(int, doc['date'].split('-')))], size=len(doc['text']))
        indexer.commit()
        query = indexer.fields['amendment'].range(None, 10)
        assert indexer.count(query) == 9
        field = indexer.fields['date']
        query = field.prefix((1791, 12))
        assert indexer.count(query) == 10
        query = field.prefix(datetime.date(1791, 12, 15))
        assert indexer.count(query) == 10
        query = field.range(None, (1921, 12), lower=False, upper=True)
        assert indexer.count(query) == 19
        query = field.range(datetime.date(1919, 1, 1), datetime.date(1921, 12, 31))
        hits = indexer.search(query)
        assert [hit['amendment'] for hit in hits] == ['18', '19']
        assert [datetime.datetime.utcfromtimestamp(float(hit['date'])).year for hit in hits] == [1919, 1920]
        assert indexer.count(field.within(seconds=100)) == indexer.count(field.within(weeks=1)) == 0
        query = field.duration([2009], days=-100*365)
        assert indexer.count(query) == 12
        field = indexer.fields['size']
        assert len(list(indexer.terms('size'))) > len(indexer)
        sizes = dict((id, int(indexer[id]['size'])) for id in indexer)
        ids = sorted((id for id in sizes if sizes[id] >= 1000), key=sizes.get)
        query = field.range(1000, None)
        hits = indexer.search(query, sort=sizes.get)
        assert hits.ids == ids
        hits = indexer.search(query, count=3, sort=engine.SortField('size', type=long))
        assert hits.ids == ids[:len(hits)]
        query = field.range(None, 1000)
        assert indexer.count(query) == len(sizes) - len(ids)
        self.assertRaises(OverflowError, list, field.items(-2**64))
        nf, = field.items(0.5)
        assert nf.numericValue.doubleValue() == 0.5
        assert str(field.range(-2**64, 0)) == 'size:[* TO 0}'
        assert str(field.range(0, 2**64)) == 'size:[0 TO *}'
        assert str(field.range(0.5, None, upper=True)) == 'size:[0.5 TO *]'
        for step, count in zip(range(0, 20, field.step), (26, 19, 3, 1)):
            sizes = list(indexer.numbers('size', step))
            assert len(sizes) == count and all(isinstance(size, int) for size in sizes)
            numbers = dict(indexer.numbers('size', step, type=float, counts=True))
            assert sum(numbers.values()) == len(indexer) and all(isinstance(number, float) for number in numbers)
    
    def testHighlighting(self):
        "Highlighting text fragments."
        indexer = engine.Indexer()
        indexer.set('text', store=True)
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
        fragment, = indexer.highlight(query, text, count=3, formatter='em')
        assert len(fragment) < len(text) and fragment.count('<em>') == 3
        indexer.add(text=text)
        indexer.commit()
        fragment, = indexer.highlight(query, 0, field='text')
        assert fragment.count('<B>') == fragment.count('</B>') == 3
    
    def testNearRealTime(self):
        "Near real-time index updates."
        indexer = engine.Indexer(version=lucene.Version.LUCENE_30, nrt=True)
        indexer.add()
        assert indexer.count() == 0 and not indexer.current
        indexer.refresh(filters=True)
        assert indexer.count() == 1 and indexer.current
        searcher = engine.IndexSearcher(indexer.directory)
        assert searcher.count() == 0 and searcher.current
        indexer.add()
        indexer.commit()
        assert indexer.count() == engine.IndexSearcher(indexer.directory).count() == 2

if __name__ == '__main__':
    lucene.initVM()
    unittest.main()
