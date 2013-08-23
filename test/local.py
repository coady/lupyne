from future_builtins import map, zip
import unittest
import os
import tempfile, shutil
import itertools
import warnings
import datetime
import math
import bisect
import contextlib
import lucene
try:
    from java.io import StringReader
    from org.apache.lucene import analysis, document, search, store, util
    from org.apache.lucene.analysis import miscellaneous, standard
    from org.apache.lucene.search import grouping, highlight, vectorhighlight
    from org.apache.pylucene.search import PythonFilter
except ImportError:
    from lucene import StringReader, PythonFilter
    analysis = document = search = store = util = miscellaneous = standard = grouping = highlight = vectorhighlight = lucene
from lupyne import engine
from . import fixture
if not hasattr(analysis, 'PorterStemFilter'):
    analysis.PorterStemFilter = analysis.en.PorterStemFilter
if hasattr(analysis, 'core'):
    analysis.WhitespaceAnalyzer, analysis.WhitespaceTokenizer = analysis.core.WhitespaceAnalyzer, analysis.core.WhitespaceTokenizer

class typeAsPayload(engine.TokenFilter):
    "Custom implementation of lucene TypeAsPayloadTokenFilter."
    def setattrs(self):
        self.payload = self.type

@contextlib.contextmanager
def assertWarns(*categories):
    with warnings.catch_warnings(record=True) as messages:
        yield
    for message, category in itertools.izip_longest(messages, categories):
         assert issubclass(message.category, category), message

class Filter(PythonFilter):
    "Broken filter to test errors are raised."
    def getDocIdSet(self, *args):
        assert False

class BaseTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(dir=os.path.dirname(__file__))
    def tearDown(self):
        shutil.rmtree(self.tempdir)

class TestCase(BaseTest):
    
    def testInterface(self):
        "Indexer and document interfaces."
        self.assertRaises(TypeError, engine.IndexSearcher)
        analyzer = lambda reader: standard.StandardTokenizer(util.Version.values()[-1], reader)
        stemmer = engine.Analyzer(analyzer, analysis.PorterStemFilter, typeAsPayload)
        for token in stemmer.tokens('hello'):
            assert token.positionIncrement == 1
            assert engine.TokenFilter(miscellaneous.EmptyTokenStream()).payload is None
            assert token.term == 'hello'
            assert token.type == token.payload == '<ALPHANUM>'
            assert token.offset == (0, 5)
            token.term = token.type = ''
            token.offset, token.positionIncrement = (0, 0), 0
        assert str(stemmer.parse('hellos', field=['body', 'title'])) == 'body:hello title:hello'
        assert str(stemmer.parse('hellos', field={'body': 1.0, 'title': 2.0})) == 'body:hello title:hello^2.0'
        indexer = engine.Indexer(analyzer=stemmer, version=util.Version.LUCENE_30, writeLockTimeout=100L)
        assert indexer.config.writeLockTimeout == 100
        self.assertRaises(lucene.JavaError, engine.Indexer, indexer.directory)
        indexer.set('text')
        indexer.set('name', store=True, index=False)
        indexer.set('tag', store=True, index=True, boost=2.0)
        for field in indexer.fields['tag'].items('sample'):
            assert isinstance(field, document.Field) and getattr(field, 'getBoost', field.boost)() == 2.0
        searcher = indexer.indexSearcher
        indexer.commit()
        assert searcher is indexer.indexSearcher
        assert not searcher.search(count=1)
        indexer.add(text='hello worlds', name='sample', tag=['python', 'search'])
        assert len(indexer) == 1 and list(indexer) == []
        indexer.commit()
        assert searcher is not indexer.indexSearcher
        assert list(indexer) == [0]
        assert indexer.current
        assert 0 in indexer and 1 not in indexer
        doc = indexer[0]
        assert doc == {'tag': ['python', 'search'], 'name': ['sample']}
        assert doc['name'] == 'sample' and doc['tag'] == 'python'
        assert doc.dict('tag') == {'name': 'sample', 'tag': ['python', 'search']}
        assert doc.dict(name=None, missing=True) == {'name': 'sample', 'missing': True}
        self.assertRaises(KeyError, doc.__getitem__, 'key')
        assert doc.getlist('name') == ['sample'] and doc.getlist('key') == []
        assert indexer.get(0, 'name').dict() == {'name': 'sample'}
        assert not list(indexer.termvector(0, 'tag'))
        assert indexer.count('text', 'hello') == indexer.count('text:hello') == 1
        assert sorted(indexer.names()) == ['name', 'tag', 'text']
        try:
            names = indexer.names(indexed=True)
        except AttributeError:
            names = indexer.names('indexed', isIndexed=True)
        assert sorted(names)[-2:] == ['tag', 'text']
        try:
            names = indexer.names(indexed=False)
        except AttributeError:
            names = indexer.names('unindexed', isIndexed=False)
        assert 'name' in names
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
        self.assertRaises(AssertionError, hits.__getitem__, slice(None, None, 2))
        assert hits.scoredocs is hits[:1].scoredocs and not hits[1:]
        assert list(hits.ids) == [0]
        score, = hits.scores
        assert 0 < score < 1
        assert dict(hits.items()) == {0: score}
        data = hits[0].dict()
        assert data['__id__'] == 0 and '__score__' in data
        assert not indexer.search('hello') and indexer.search('hello', field='text')
        assert indexer.search('text:hello hi') and not indexer.search('text:hello hi', op='and')
        assert indexer.search('text:*hello', allowLeadingWildcard=True)
        query = engine.Query.multiphrase('text', ('hello', 'hi'), None, 'world')
        assert str(query).startswith('text:"(hello hi) ') and list(query.positions) == [0, 2]
        query = engine.Query.wildcard('text', '*')
        assert str(query) == 'text:*' and isinstance(query, search.WildcardQuery)
        assert str(search.MatchAllDocsQuery() | query) == '*:* text:*'
        assert str(search.MatchAllDocsQuery() - query) == '*:* -text:*'
        query = +query
        if hasattr(search.FuzzyQuery, 'defaultMaxEdits'):
            query &= engine.Query.fuzzy('text', 'hello')
            query |= engine.Query.fuzzy('text', 'hello', 1)
            assert str(query) == '+text:* +text:hello~2 text:hello~1'
        else:
            query &= engine.Query.fuzzy('text', 'hello')
            query |= engine.Query.fuzzy('text', 'hello', 0.1)
            assert str(query) == '+text:* +text:hello~0.5 text:hello~0.1'
        query = engine.Query.span('text', 'world')
        assert str(query.mask('name')) == 'mask(text:world) as name'
        assert str(query.payload()) == 'spanPayCheck(text:world, payloadRef: )'
        assert isinstance(query.filter(cache=False), getattr(search, 'SpanQueryFilter', search.QueryWrapperFilter))
        assert isinstance(query.filter(), getattr(search, 'CachingSpanFilter', search.CachingWrapperFilter))
        query = engine.Query.disjunct(0.1, query, name='sample')
        assert str(query) == '(text:world | name:sample)~0.1'
        query = engine.Query.near('text', 'hello', ('tag', 'python'), slop=-1, inOrder=False)
        assert str(query) == 'spanNear([text:hello, mask(tag:python) as text], -1, false)' and indexer.count(query) == 1
        query = engine.Query.near('text', 'hello', 'world')
        (doc, items), = indexer.spans(query, payloads=True)
        (start, stop, payloads), = items
        assert doc == 0 and start == 0 and stop == 2 and payloads == ['<ALPHANUM>', '<ALPHANUM>']
        (doc, count), = indexer.spans(query.payload('<ALPHANUM>', '<ALPHANUM>'))
        assert doc == 0 and count == 1
        assert not indexer.search(query.payload('<>'))
        query = engine.Query.near('text', 'hello', 'world', collectPayloads=False)
        (doc, items), = indexer.spans(query, payloads=True)
        assert doc == 0 and items == []
        indexer.delete('name:sample')
        indexer.delete('tag', 'python')
        assert 0 in indexer and len(indexer) == 1 and indexer.segments == {'_0': 1}
        indexer.commit()
        assert 0 not in indexer and len(indexer) == 0 and sum(indexer.segments.values()) == 0
        indexer.add(tag='test', name='old')
        indexer.update('tag', tag='test')
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
        indexer.add(text=analysis.WhitespaceTokenizer(util.Version.LUCENE_CURRENT, StringReader('?')), name=lucene.JArray_byte('{}'))
        indexer.commit()
        value = indexer[next(indexer.docs('text', '?'))]['name']
        assert value == '{}' or value.utf8ToString() == '{}'
        reader = engine.indexers.IndexReader(indexer.indexReader)
        assert reader[0].dict() == {} and reader.count('text', '?') == 1
        assert len(reader.comparator('text')) == 4
        indexer.delete('text', '?')
        indexer.commit(merge=True)
        assert not indexer.hasDeletions()
        indexer.commit(merge=1)
        assert len(list(indexer.readers)) == 1
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
            indexer.add(doc)
        indexer.commit()
        searcher = engine.IndexSearcher.load(self.tempdir)
        engine.IndexSearcher.load(searcher.directory) # ensure directory isn't closed
        assert len(indexer) == len(searcher) and store.RAMDirectory.instance_(searcher.directory)
        assert indexer.filters == indexer.spellcheckers == {}
        assert indexer.facets(search.MatchAllDocsQuery(), 'amendment')
        assert indexer.suggest('amendment', '')
        assert list(indexer.filters) == list(indexer.spellcheckers) == ['amendment']
        indexer.delete('amendment', doc['amendment'])
        indexer.add(doc)
        reader = indexer.indexReader
        indexer.commit(filters=True, spellcheckers=True)
        assert reader.refCount == 0
        assert list(indexer.filters) == list(indexer.spellcheckers) == ['amendment']
        tokenizer = lambda reader: analysis.WhitespaceTokenizer(util.Version.LUCENE_CURRENT, reader)
        doc['amendment'] = engine.Analyzer(tokenizer).tokens(doc['amendment'])
        doc['date'] = engine.Analyzer(tokenizer).tokens(doc['date']), 2.0
        scores = list(searcher.match(doc, 'text:congress', 'text:law', 'amendment:27', 'date:19*'))
        assert 0.0 == scores[0] < scores[1] < scores[2] < scores[3] == 1.0
        searcher = engine.MultiSearcher([indexer.indexReader, self.tempdir])
        assert searcher.refCount == 1 and searcher.timestamp
        assert searcher.count() == len(searcher) == 2 * len(indexer)
        searcher.sorters['amendment'] = engine.SortField('amenmdment', int)
        comparator = searcher.comparator('amendment')
        assert set(map(type, comparator)) == set([int])
        assert searcher is searcher.reopen()
        assert searcher.facets(search.MatchAllDocsQuery(), 'amendment')['amendment'] == dict.fromkeys(map(str, range(1, 28)), 2)
        reader = searcher.indexReader
        del searcher
        assert not reader.refCount
        assert len(indexer) == len(indexer.search()) == 35
        assert sorted(indexer.names()) == ['amendment', 'article', 'date', 'text']
        articles = list(indexer.terms('article'))
        articles.remove('Preamble')
        assert sorted(map(int, articles)) == range(1, 8)
        assert sorted(map(int, indexer.terms('amendment'))) == range(1, 28)
        assert list(itertools.islice(indexer.terms('text', 'right'), 2)) == ['right', 'rights']
        assert list(indexer.terms('text', 'right*')) == ['right', 'rights']
        if hasattr(search, 'WildcardTermEnum'):
            with assertWarns(DeprecationWarning):
                assert list(indexer.terms('text', 'right?')) == ['rights']
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
        assert 'Preamble' in (hit.get('article') for hit in hits)
        assert len(hits) == hits.count == 8
        assert set(map(type, hits.ids)) == set([int]) and set(map(type, hits.scores)) == set([float])
        assert hits.maxscore == max(hits.scores)
        ids = list(hits.ids)
        hits = indexer.search('people', count=5, field='text')
        assert list(hits.ids) == ids[:len(hits)]
        assert len(hits) == 5 and hits.count == 8
        assert not any(map(math.isnan, hits.scores))
        assert hits.maxscore == max(hits.scores)
        hits = indexer.search('text:people', count=5, sort=search.Sort.INDEXORDER)
        assert sorted(hits.ids) == list(hits.ids)
        sort = engine.SortField('amendment', type=int)
        hits = indexer.search('text:people', count=5, sort=sort)
        assert [hit.get('amendment') for hit in hits] == [None, None, '1', '2', '4']
        assert [key for hit in hits for key in hit.keys]== [0, 0, 1, 2, 4]
        assert all(map(math.isnan, hits.scores))
        hits = indexer.search('text:right', count=10**7, sort=sort, scores=True)
        assert not any(map(math.isnan, hits.scores)) and sorted(hits.scores, reverse=True) != hits.scores
        assert math.isnan(hits.maxscore)
        hits = indexer.search('text:right', count=2, sort=sort, maxscore=True)
        assert hits.maxscore > max(hits.scores)
        parser = lambda value: int((value.utf8ToString() if hasattr(value, 'utf8ToString') else value) or -1)
        comparator = indexer.comparator('amendment', type=int, parser=parser)
        hits = indexer.search('text:people').sorted(comparator.__getitem__)
        assert sorted(hits.ids) == list(hits.ids) and list(hits.ids) != ids
        comparator = list(zip(*map(indexer.comparator, ['article', 'amendment'])))
        hits = indexer.search('text:people').sorted(comparator.__getitem__)
        assert sorted(hits.ids) != list(hits.ids)
        hits = indexer.search('text:people', count=5, sort='amendment', reverse=True)
        assert [hit['amendment'] for hit in hits] == ['9', '4', '2', '17', '10']
        hit, = indexer.search('freedom', field='text')
        assert hit['amendment'] == '1'
        assert sorted(hit.dict()) == ['__id__', '__score__', 'amendment', 'date']
        hits = indexer.search('text:right')
        for name in ('amendment', 'article'):
            indexer.filters[name] = engine.Query.prefix(name, '').filter()
        query = engine.Query.term('text', 'right', boost=2.0)
        assert query.boost == 2.0
        assert indexer.facets(str(query), 'amendment', 'article') == {'amendment': 12, 'article': 1}
        hits = indexer.search('text:people', filter=query.filter())
        assert len(hits) == 4
        hit, = indexer.search('date:192*')
        assert hit['amendment'] == '19'
        hits = indexer.search('date:[1919 TO 1921]')
        amendments = ['18', '19']
        assert sorted(hit['amendment'] for hit in hits) == amendments
        query = engine.Query.range('date', '1919', '1921')
        hits = indexer.search(filter=query.filter())
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
        assert ('text', 'persons') in query.terms()
        assert str(query[-1]) == '-text:papers'
        assert len(query) == len(list(query)) == 2
        span = engine.Query.span('text', 'persons')
        count = indexer.count(span)
        assert indexer.count(engine.Query.span(engine.Query.prefix('text', 'person'))) > count
        near = engine.Query.near('text', 'persons', 'papers', slop=1, inOrder=False)
        assert indexer.count(span - near) == count
        near = span.near(engine.Query.span('text', 'papers') | engine.Query.span('text', 'things'), slop=1)
        assert indexer.count(span - near) == count - 1
        assert 0 < indexer.count(span[:100]) < count
        assert 0 < indexer.count(span[50:100]) == indexer.count(span[:100] - span[:50]) < indexer.count(span[:100])
        spans = dict(indexer.spans(span))
        assert len(spans) == count and spans == dict(indexer.docs('text', 'persons', counts=True))
        near = engine.Query.near('text', 'persons', 'papers', slop=2)
        (id, positions), = indexer.spans(near, positions=True)
        assert indexer[id]['amendment'] == '4' and positions in ([(3, 6)], [(10, 13)])
        assert 'persons' in indexer.termvector(id, 'text')
        assert dict(indexer.termvector(id, 'text', counts=True))['persons'] == 2
        assert dict(indexer.positionvector(id, 'text'))['persons'] in ([3, 26], [10, 48])
        assert dict(indexer.positionvector(id, 'text', offsets=True))['persons'] == [(46, 53), (301, 308)]
        analyzer = analysis.WhitespaceAnalyzer(util.Version.LUCENE_CURRENT)
        query = indexer.morelikethis(0, analyzer=analyzer)
        assert str(query) == 'text:united text:states'
        hits = indexer.search(query & engine.Query.prefix('article', ''))
        assert len(hits) == 8 and hits[0]['article'] == 'Preamble'
        assert str(indexer.morelikethis(0, 'article', analyzer=analyzer)) == ''
        assert str(indexer.morelikethis(0, minDocFreq=3, analyzer=analyzer)) == 'text:establish text:united text:states'
        assert str(indexer.morelikethis('jury', 'text', minDocFreq=4, minTermFreq=1, analyzer=analyzer)) == 'text:jury'
        assert str(indexer.morelikethis('jury', 'article', analyzer=analyzer)) == ''
        try:
            query = indexer.morelikethis('jury')
        except lucene.JavaError:
            pass
        else:
            assert str(query) == ''
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
        assert search.TermQuery.instance_(query) and str(query) == 'text:writs'
        query = indexer.parse('"hello world"', field='text', spellcheck=True)
        assert search.PhraseQuery.instance_(query) and str(query) == 'text:"held would"'
        assert str(indexer.parse('vwxyz', field='text', spellcheck=True)) == 'text:vwxyz'
        files = set(os.listdir(self.tempdir))
        path = os.path.join(self.tempdir, 'temp')
        with indexer.snapshot('backup') as commit:
            indexer.commit(merge=1)
            assert indexer.indexCommit.generation > commit.generation
            engine.indexers.copy(commit, path)
            assert set(os.listdir(path)) == set(commit.fileNames) < files < set(os.listdir(self.tempdir))
            filepath = os.path.join(path, commit.segmentsFileName)
            os.remove(filepath)
            open(filepath, 'w').close()
            self.assertRaises(OSError, engine.indexers.copy, commit, path)
        del indexer
        assert engine.Indexer(self.tempdir)
        assert not os.path.exists(os.path.join(self.tempdir, commit.segmentsFileName))
    
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
        try:
            names = indexer.names(indexed=True)
        except AttributeError:
            names = indexer.names('indexed', isIndexed=True)
        assert set(['state', 'zipcode']) < set(names)
        try:
            names = indexer.names(indexed=False)
        except AttributeError:
            names = indexer.names('unindexed', isIndexed=False)
        assert set(['latitude', 'longitude', 'county', 'city']) <= set(names)
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
        assert all(value.startswith('CA.') for value in facets) and set(facets) < set(indexer.filters[field])
        assert set(indexer.grouping('state', count=1)) < set(indexer.grouping('state')) == set(states)
        grouper = indexer.grouping(field, query, sort=search.Sort(indexer.sorter(field)))
        assert len(grouper) == 2 and list(grouper) == [la, orange]
        for value, (name, count) in zip(grouper, grouper.facets(None)):
            assert value == name and count > 0
        grouper = indexer.groupings[field] = indexer.grouping(field, engine.Query.term('state', 'CA'))
        assert indexer.facets(query, field)[field] == facets
        hits = next(grouper.groups())
        assert hits.value == 'CA.Los Angeles' and hits.count > 100 and len(hits) == 1
        hit, = hits
        assert hit.score in hit.keys
        assert hit['county'] == 'Los Angeles' and hits.maxscore >= hit.score > 0
        hits = next(grouper.groups(count=2, sort=search.Sort(indexer.sorter('zipcode')), scores=True))
        assert hits.value == 'CA.Los Angeles' and math.isnan(hits.maxscore) and len(hits) == 2
        assert all(hit.score > 0 and hit['zipcode'] > '90000' and hit['zipcode'] in hit.keys for hit in hits)
        for count in (None, len(indexer)):
            hits = indexer.search(query, count=count, timeout=0.01)
            assert 0 <= len(hits) <= indexer.count(query) and hits.count in (None, len(hits)) and hits.maxscore in (None, 1.0)
            hits = indexer.search(query, count=count, timeout=-1)
            assert len(hits) == 0 and hits.count is hits.maxscore is None
        self.assertRaises(lucene.JavaError, indexer.search, filter=Filter())
        directory = store.RAMDirectory()
        query = engine.Query.term('state', 'CA')
        size = indexer.copy(directory, query)
        searcher = engine.IndexSearcher(directory)
        assert len(searcher) == size and list(searcher.terms('state')) == ['CA']
        path = os.path.join(self.tempdir, 'temp')
        size = indexer.copy(path, exclude=query, merge=1)
        assert len(searcher) + size == len(indexer)
        searcher = engine.IndexSearcher(path)
        assert len(searcher.segments) == 1 and 'CA' not in searcher.terms('state')
        directory.close()
    
    def testSpatial(self):
        "Spatial tiles."
        indexer = engine.Indexer(self.tempdir, 'w')
        for name, params in fixture.zipcodes.fields.items():
            indexer.set(name, **params)
        for name in ('longitude', 'latitude'):
            indexer.set(name, engine.NumericField, store=True)
        field = indexer.fields['tile'] = engine.PointField('tile', precision=15, step=2, store=True)
        points = []
        for doc in fixture.zipcodes.docs():
            if doc['state'] == 'CA':
                point = doc['longitude'], doc['latitude']
                indexer.add(doc, tile=[point])
                if doc['city'] == 'Los Angeles':
                    points.append(point)
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
        distances = indexer.distances(x, y, 'longitude', 'latitude')
        hits = indexer.search(query).sorted(distances.__getitem__)
        assert hits[0]['zipcode'] == zipcode and distances[hits[0].id] < 10
        cities = set(hit['city'] for hit in hits)
        assert city in cities and 100 > len(cities) > 50
        hits = indexer.search(field.within(x, y, 10**5, limit=100))
        cities = set(hit['city'] for hit in hits)
        assert city in cities and len(cities) > 100
        ranges = 10**2, 10**5
        groups = hits.groupby(lambda id: bisect.bisect_left(ranges, distances[id]))
        counts = dict((hits.value, len(hits)) for hits in groups)
        assert 1 == counts[0] < counts[2] < counts[1]
        assert len(field.within(x, y, 10**8)) == 1
        self.assertRaises(NameError, list, field.radiate(y, x, 1, 0))
        hits = hits.filter(lambda id: distances[id] < 10**4)
        assert 0 < len(hits) < sum(counts.values())
        hits = hits.sorted(distances.__getitem__, reverse=True)
        ids = list(hits.ids)
        assert 0 == distances[ids[-1]] < distances[ids[0]] < 10**4
    
    def testFields(self):
        "Custom fields."
        self.assertRaises(lucene.JavaError, engine.Field, '', store='invalid')
        self.assertRaises(AttributeError, engine.Field, '', omit='value')
        self.assertRaises(lucene.JavaError, engine.Field, '', index=False)
        field = engine.Field('', index=True, analyzed=True, omitNorms=True, termvector=True, withPositions=True, withOffsets=True)
        field, = field.items(' ')
        attrs = 'indexed', 'tokenized', 'termVectorStored', 'storePositionWithTermVector', 'storeOffsetWithTermVector', 'omitNorms'
        try:
            assert all(getattr(field, attr) for attr in attrs)
        except AttributeError:
            attrs = 'indexed', 'tokenized', 'storeTermVectors', 'storeTermVectorPositions', 'storeTermVectorOffsets', 'omitNorms'
            assert all(getattr(field.fieldType(), attr)() for attr in attrs)
        indexer = engine.Indexer(self.tempdir)
        indexer.set('amendment', engine.MapField, func='{0:02d}'.format, store=True)
        indexer.set('size', engine.MapField, func='{0:04d}'.format, store=True)
        field = indexer.fields['date'] = engine.NestedField('Y-m-d', sep='-', store=True)
        for doc in fixture.constitution.docs():
            if 'amendment' in doc:
                indexer.add(amendment=int(doc['amendment']), date=doc['date'], size=len(doc['text']))
        indexer.commit()
        query = engine.Query.range('amendment', '', indexer.fields['amendment'].func(10))
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
        hits = indexer.search(query).sorted(sizes.get)
        assert list(hits.ids) == ids
        hits = indexer.search(query, count=3, sort=engine.SortField('size', type=long))
        assert list(hits.ids) == ids[:len(hits)]
        query = engine.Query.range('size', None, '1000')
        assert indexer.count(query) == len(sizes) - len(ids)
        parser = lambda date: int((date.utf8ToString() if lucene.VERSION >= '4' else date).split('-')[0])
        indexer.sorters['year'] = engine.SortField('Y-m-d', type=int, parser=parser)
        assert list(indexer.comparator('year'))[:10] == [1791] * 10
        cache = len(search.FieldCache.DEFAULT.cacheEntries)
        hits = indexer.search(count=3, sort='year')
        assert [int(hit['amendment']) for hit in hits] == [1, 2, 3]
        hits = indexer.search(count=3, sort='year', reverse=True)
        assert [int(hit['amendment']) for hit in hits] == [27, 26, 25]
        filter = indexer.sorters['year'].filter(None, 1792)
        assert indexer.count(filter=filter) == 10
        assert set(indexer.sorters['year'].terms(filter, *indexer.readers)) == set([1791])
        assert cache == len(search.FieldCache.DEFAULT.cacheEntries)
        indexer.add()
        indexer.commit(sorters=True)
        cache = len(search.FieldCache.DEFAULT.cacheEntries)
        assert list(indexer.comparator('year'))[-1] == 0
        assert cache == len(search.FieldCache.DEFAULT.cacheEntries)
        self.assertRaises(AttributeError, indexer.comparator, 'size', type='score')
    
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
        assert indexer.count(filter=indexer.fields['amendment'].filter(None, 10)) == 9
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
        hits = indexer.search(query).sorted(sizes.get)
        assert list(hits.ids) == ids
        hits = indexer.search(query, count=3, sort=engine.SortField('size', type=long))
        assert list(hits.ids) == ids[:len(hits)]
        query = field.range(None, 1000)
        assert indexer.count(query) == len(sizes) - len(ids)
        self.assertRaises(OverflowError, list, field.items(-2**64))
        nf, = field.items(0.5)
        assert (nf.numericValue if hasattr(document, 'NumericField') else nf.numericValue()).doubleValue() == 0.5
        assert str(field.range(-2**64, 0)) == 'size:[* TO 0}'
        assert str(field.range(0, 2**64)) == 'size:[0 TO *}'
        assert str(field.range(0.5, None, upper=True)) == 'size:[0.5 TO *]'
        for step, count in zip(range(0, 20, field.step), (26, 19, 3, 1)):
            sizes = list(indexer.numbers('size', step))
            assert len(sizes) == count and all(isinstance(size, int) for size in sizes)
            numbers = dict(indexer.numbers('size', step, type=float, counts=True))
            assert sum(numbers.values()) == len(indexer) and all(isinstance(number, float) for number in numbers)
        hit, = indexer.search(indexer.fields['amendment'].term(1))
        assert hit['amendment'] == '1'
    
    def testHighlighting(self):
        "Highlighting text fragments."
        indexer = engine.Indexer()
        indexer.set('text', store=True, termvector=True, withPositions=True, withOffsets=True)
        for doc in fixture.constitution.docs():
            if 'amendment' in doc:
                indexer.add(text=doc['text'])
        indexer.commit()
        highlighter = indexer.highlighter('persons', 'text')
        for id in indexer:
            fragments = highlighter.fragments(id)
            assert len(fragments) == ('persons' in indexer[id]['text'])
            assert all('<b>persons</b>' in fragment.lower() for fragment in fragments)
        id = 3
        text = indexer[id]['text']
        query = '"persons, houses, papers"'
        highlighter = indexer.highlighter(query, '', terms=True, fields=True, formatter=highlight.SimpleHTMLFormatter('*', '*'))
        fragments = highlighter.fragments(text, count=3)
        assert len(fragments) == 2 and fragments[0].count('*') == 2*3 and '*persons*' in fragments[1]
        highlighter = indexer.highlighter(query, '', terms=True)
        highlighter.textFragmenter = highlight.SimpleFragmenter(200)
        fragment, = highlighter.fragments(text, count=3)
        assert len(fragment) > len(text) and fragment.count('<B>persons</B>') == 2
        fragment, = indexer.highlighter(query, 'text', tag='em').fragments(id, count=3)
        assert len(fragment) < len(text) and fragment.index('<em>persons') < fragment.index('papers</em>')
        fragment, = indexer.highlighter(query, 'text').fragments(id)
        assert fragment.count('<b>') == fragment.count('</b>') == 1
        highlighter = indexer.highlighter(query, 'text', fragListBuilder=vectorhighlight.SingleFragListBuilder())
        text, = highlighter.fragments(id)
        assert fragment in text and len(text) > len(fragment)
    
    def testNearRealTime(self):
        "Near real-time index updates."
        indexer = engine.Indexer(version=util.Version.LUCENE_30, nrt=True)
        indexer.add()
        assert indexer.count() == 0 and not indexer.current
        indexer.refresh(filters=True)
        assert indexer.count() == 1 and indexer.current
        searcher = engine.IndexSearcher(indexer.directory)
        assert searcher.count() == 0 and searcher.current
        indexer.add()
        indexer.commit()
        assert indexer.count() == engine.IndexSearcher(indexer.directory).count() == 2
    
    def testFilters(self):
        "Custom filters."
        indexer = engine.Indexer()
        indexer.set('name', store=True, index=True)
        for name in ('alpha', 'bravo', 'charlie'):
            indexer.add(name=name)
        indexer.commit()
        filter = engine.TermsFilter('name')
        assert len(filter.readers) == 0
        filter.add('alpha', 'bravo')
        filter.discard('bravo', 'charlie')
        assert filter.values == set(['alpha'])
        parallel = engine.ParallelIndexer('name')
        parallel.set('priority', index=True)
        for name in ('alpha', 'bravo', 'delta'):
            parallel.update(name, priority='high')
        parallel.commit()
        filter = parallel.termsfilter(engine.Query.term('priority', 'high').filter(), indexer)
        assert [hit['name'] for hit in indexer.search(filter=filter)] == ['alpha', 'bravo']
        indexer.add(name='delta')
        indexer.delete('name', 'alpha')
        indexer.commit()
        assert filter.readers > set(indexer.readers)
        assert [hit['name'] for hit in indexer.search(filter=filter)] == ['bravo', 'delta']
        parallel.update('bravo')
        parallel.update('charlie', priority='high')
        parallel.commit()
        assert [hit['name'] for hit in indexer.search(filter=filter)] == ['charlie', 'delta']
        parallel.commit()
        filter.refresh(indexer)
        assert filter.readers == set(indexer.readers)

if __name__ == '__main__':
    lucene.initVM()
    unittest.main()
