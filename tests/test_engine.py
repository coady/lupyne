from future_builtins import map, zip
import os
import itertools
import datetime
import math
import bisect
import pytest
import lucene
from java.io import StringReader
from org.apache.lucene import analysis, document, search, store, util
from org.apache.lucene.search import highlight, vectorhighlight
from lupyne import engine

Q = engine.Query
tokenizer = analysis.standard.StandardTokenizer


class typeAsPayload(engine.TokenFilter):
    "Custom implementation of lucene TypeAsPayloadTokenFilter."
    def setattrs(self):
        self.payload = self.type


@pytest.fixture
def indexer(tempdir):
    with engine.Indexer(tempdir) as indexer:
        for name in ('city', 'county', 'state', 'latitude', 'longitude'):
            indexer.set(name, stored=True)
        indexer.set('zipcode', engine.Field.String, stored=True)
        yield indexer


def test_interface(tempdir):
    with pytest.raises(TypeError):
        engine.IndexSearcher()
    stemmer = engine.Analyzer(tokenizer, analysis.en.PorterStemFilter, typeAsPayload)
    for token in stemmer.tokens('hello'):
        assert token.positionIncrement == 1
        assert engine.TokenFilter(analysis.miscellaneous.EmptyTokenStream()).payload is None
        assert token.term == 'hello'
        assert token.type == token.payload == '<ALPHANUM>'
        assert token.offset == (0, 5)
        token.term = token.type = ''
        token.offset, token.positionIncrement = (0, 0), 0
    assert str(stemmer.parse('hellos', field=['body', 'title'])) == 'body:hello title:hello'
    assert str(stemmer.parse('hellos', field={'body': 1.0, 'title': 2.0})) == 'body:hello title:hello^2.0'
    indexer = engine.Indexer(analyzer=stemmer, writeLockTimeout=100L)
    assert indexer.config.writeLockTimeout == 100
    with pytest.raises(lucene.JavaError):
        engine.Indexer(indexer.directory)
    indexer.set('text', engine.Field.Text)
    indexer.set('name', stored=True)
    indexer.set('tag', engine.Field.Text, stored=True, boost=2.0)
    for field in indexer.fields['tag'].items('sample'):
        assert isinstance(field, document.Field) and field.boost() == 2.0
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
    with pytest.raises(KeyError):
        doc['key']
    assert doc.getlist('name') == ['sample'] and doc.getlist('key') == []
    assert indexer.get(0, 'name').dict() == {'name': 'sample'}
    assert not list(indexer.termvector(0, 'tag'))
    assert indexer.count('text', 'hello') == indexer.count('text:hello') == 1
    assert list(indexer.terms('text')) == ['hello', 'world']
    assert list(indexer.terms('text', 'h', 'v')) == ['hello']
    assert dict(indexer.terms('text', 'w', counts=True)) == {'world': 1}
    assert list(indexer.terms('test')) == []
    assert list(indexer.docs('text', 'hello')) == [0]
    assert list(indexer.docs('text', 'hi')) == []
    assert list(indexer.docs('text', 'world', counts=True)) == [(0, 1)]
    assert list(indexer.positions('text', 'world')) == [(0, [1])]
    assert list(indexer.positions('text', 'world', payloads=True)) == [(0, [(1, '<ALPHANUM>')])]
    assert list(indexer.positions('text', 'world', offsets=True)) == [(0, [(-1, -1)])]
    hits = indexer.search('text:hello')
    assert len(hits) == hits.count == 1
    with pytest.raises(AssertionError):
        hits[slice(None, None, 2)]
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
    query = Q.near('text', 'hello', 'world')
    (doc, items), = indexer.spans(query, payloads=True)
    (start, stop, payloads), = items
    assert doc == 0 and start == 0 and stop == 2 and payloads == ['<ALPHANUM>', '<ALPHANUM>']
    (doc, count), = indexer.spans(query.payload('<ALPHANUM>', '<ALPHANUM>'))
    assert doc == 0 and count == 1
    assert not indexer.search(query.payload('<>'))
    query = Q.near('text', 'hello', 'world', collectPayloads=False)
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
    with engine.Indexer(tempdir) as temp:
        temp.add()
    try:
        with engine.Indexer(tempdir) as temp:
            temp.add()
            temp.add(missing='')
    except KeyError:
        pass
    for other in (temp, temp.directory, tempdir):
        indexer += other
    assert len(indexer) == 3
    indexer.add(text=analysis.core.WhitespaceTokenizer(StringReader('?')), name=util.BytesRef('{}'))
    indexer.commit()
    assert indexer[next(indexer.docs('text', '?'))]['name'] == '{}'
    indexer.delete('text', '?')
    indexer.commit(merge=True)
    assert not indexer.hasDeletions()
    indexer.commit(merge=1)
    assert len(list(indexer.readers)) == 1
    reader = engine.indexers.IndexReader(indexer.indexReader)
    del reader.indexReader
    with pytest.raises(AttributeError):
        reader.maxDoc
    del indexer.indexSearcher
    with pytest.raises(AttributeError):
        indexer.search


def test_basic(tempdir, fields, constitution):
    with pytest.raises(lucene.JavaError):
        engine.Indexer(tempdir, 'r')
    indexer = engine.Indexer(tempdir)
    indexer.fields = {field.name: field for field in fields}
    for doc in constitution:
        indexer.add(doc)
    indexer.commit()
    searcher = engine.IndexSearcher.load(tempdir)
    engine.IndexSearcher.load(searcher.directory)  # ensure directory isn't closed
    assert len(indexer) == len(searcher) and store.RAMDirectory.instance_(searcher.directory)
    assert indexer.spellcheckers == {}
    assert indexer.facets(Q.alldocs(), 'amendment')
    assert indexer.suggest('amendment', '')
    assert list(indexer.spellcheckers) == ['amendment']
    indexer.delete('amendment', doc['amendment'])
    indexer.add(doc)
    reader = indexer.indexReader
    indexer.commit(spellcheckers=True)
    assert reader.refCount == 0
    assert list(indexer.spellcheckers) == ['amendment']
    doc = {
        'text': doc['text'],
        'amendment': engine.Analyzer(tokenizer).tokens(doc['amendment']),
        'date': (engine.Analyzer(tokenizer).tokens(doc['date']), 2.0),
    }
    scores = list(searcher.match(doc, 'text:congress', 'text:law', 'amendment:27', 'date:19*'))
    assert 0.0 == scores[0] < scores[1] < scores[2] < scores[3] == 1.0
    assert len(indexer) == len(indexer.search()) == 35
    articles = list(indexer.terms('article'))
    articles.remove('Preamble')
    assert sorted(map(int, articles)) == range(1, 8)
    assert sorted(map(int, indexer.terms('amendment'))) == range(1, 28)
    assert list(itertools.islice(indexer.terms('text', 'right'), 2)) == ['right', 'rights']
    assert list(indexer.terms('text', 'right')) == ['right', 'rights']
    assert list(indexer.terms('text', 'right', distance=1)) == ['eight', 'right', 'rights']
    assert list(indexer.terms('text', 'senite', distance=2)) == ['senate', 'sent']
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
    assert set(map(type, hits.ids)) == {int} and set(map(type, hits.scores)) == {float}
    assert hits.maxscore == max(hits.scores)
    ids = list(hits.ids)
    hits = indexer.search('people', count=5, field='text')
    assert list(hits.ids) == ids[:len(hits)]
    assert len(hits) == 5 and hits.count == 8
    assert not any(map(math.isnan, hits.scores))
    assert hits.maxscore == max(hits.scores)
    hits = indexer.search('text:people', count=5, sort=search.Sort.INDEXORDER)
    assert sorted(hits.ids) == list(hits.ids)
    hit, = indexer.search('freedom', field='text')
    assert hit['amendment'] == '1'
    assert sorted(hit.dict()) == ['__id__', '__score__', 'amendment', 'date']
    hits = indexer.search('text:right')
    hits = indexer.search('date:[1919 TO 1921]')
    amendments = ['18', '19']
    assert sorted(hit['amendment'] for hit in hits) == amendments
    query = Q.range('date', '1919', '1921')
    hits = indexer.search(query | Q.term('text', 'vote'))
    assert {hit.get('amendment') for hit in hits} > set(amendments)
    hit, = indexer.search(query & Q.term('text', 'vote'))
    assert hit['amendment'] == '19'
    hit, = indexer.search(query - Q.all(text='vote'))
    assert hit['amendment'] == '18'
    hit, = indexer.search(Q.all(text=['persons', 'papers']))
    assert hit['amendment'] == '4'
    hit, = indexer.search(Q.phrase('text', 'persons', None, 'papers', slop=1))
    assert hit['amendment'] == '4'
    hit, = indexer.search(Q.multiphrase('text', 'persons', ['houses', 'papers']))
    assert hit['amendment'] == '4'
    span = Q.span('text', 'persons')
    count = indexer.count(span)
    near = span.near(Q.span('text', 'papers') | Q.span('text', 'things'), slop=1)
    spans = dict(indexer.spans(span))
    assert len(spans) == count and spans == dict(indexer.docs('text', 'persons', counts=True))
    near = Q.near('text', 'persons', 'papers', slop=2)
    (id, positions), = indexer.spans(near, positions=True)
    assert indexer[id]['amendment'] == '4' and positions in ([(3, 6)], [(10, 13)])
    assert 'persons' in indexer.termvector(id, 'text')
    assert dict(indexer.termvector(id, 'text', counts=True))['persons'] == 2
    assert dict(indexer.positionvector(id, 'text'))['persons'] in ([3, 26], [10, 48])
    assert dict(indexer.positionvector(id, 'text', offsets=True))['persons'] == [(46, 53), (301, 308)]
    analyzer = analysis.core.WhitespaceAnalyzer()
    query = indexer.morelikethis(0, analyzer=analyzer)
    assert set(str(query).split()) == {'text:united', 'text:states'}
    hits = indexer.search(query & Q.prefix('article', ''))
    assert len(hits) == 8 and hits[0]['article'] == 'Preamble'
    assert str(indexer.morelikethis(0, 'article', analyzer=analyzer)) == ''
    query = indexer.morelikethis(0, minDocFreq=3, analyzer=analyzer)
    assert set(str(query).split()) == {'text:establish', 'text:united', 'text:states'}
    assert str(indexer.morelikethis('jury', 'text', minDocFreq=4, minTermFreq=1, analyzer=analyzer)) == 'text:jury'
    assert str(indexer.morelikethis('jury', 'article', analyzer=analyzer)) == ''
    with pytest.raises(lucene.JavaError):
        indexer.morelikethis('jury')
    assert indexer.suggest('missing', '') == list(indexer.correct('missing', '')) == []
    assert indexer.suggest('text', '')[:8] == ['shall', 'states', 'any', 'have', 'united', 'congress', 'state', 'constitution']
    assert indexer.suggest('text', 'con')[:2] == ['congress', 'constitution']
    assert indexer.suggest('text', 'congress') == indexer.suggest('text', 'con', count=1) == ['congress']
    assert indexer.suggest('text', 'congresses') == []
    assert list(indexer.correct('text', 'writ', distance=0)) == ['writ']
    assert list(indexer.correct('text', 'write', distance=0)) == []
    assert list(indexer.correct('text', 'write', distance=1)) == ['writs', 'writ']
    assert list(indexer.correct('text', 'write')) == ['writs', 'writ', 'crime', 'written']
    query = indexer.parse('text:write', spellcheck=True)
    assert search.TermQuery.instance_(query) and str(query) == 'text:writs'
    query = indexer.parse('"hello world"', field='text', spellcheck=True)
    assert search.PhraseQuery.instance_(query) and str(query) == 'text:"held would"'
    assert str(indexer.parse('vwxyz', field='text', spellcheck=True)) == 'text:vwxyz'
    files = set(os.listdir(tempdir))
    path = os.path.join(tempdir, 'temp')
    with indexer.snapshot() as commit:
        indexer.commit(merge=1)
        assert indexer.indexCommit.generation > commit.generation
        engine.indexers.copy(commit, path)
        assert set(os.listdir(path)) == set(commit.fileNames) < files < set(os.listdir(tempdir))
        filepath = os.path.join(path, commit.segmentsFileName)
        os.remove(filepath)
        open(filepath, 'w').close()
        with pytest.raises(OSError):
            engine.indexers.copy(commit, path)
    with pytest.raises(AssertionError):
        indexer.check(tempdir)
    del indexer
    assert engine.Indexer(tempdir)
    assert not os.path.exists(os.path.join(tempdir, commit.segmentsFileName))
    assert engine.IndexWriter.check(tempdir).clean
    assert not engine.IndexWriter.check(tempdir, fix=True).numBadSegments


def test_queries():
    alldocs = search.MatchAllDocsQuery()
    term = Q.term('text', 'lucene')
    assert str(term) == 'text:lucene'
    assert str(term.constant()) == 'ConstantScore(text:lucene)'
    assert str(+term) == '+text:lucene'
    assert str(-term) == '-text:lucene'
    assert str(term & alldocs) == '+text:lucene +*:*'
    assert str(alldocs & term) == '+*:* +text:lucene'
    assert str(term | alldocs) == 'text:lucene *:*'
    assert str(alldocs | term) == '*:* text:lucene'
    assert str(term - alldocs) == 'text:lucene -*:*'
    assert str(alldocs - term) == '*:* -text:lucene'

    assert str(Q.term('text', 'lucene', boost=2.0)) == 'text:lucene^2.0'
    assert str(Q.any(term, text='search')) == 'text:lucene text:search'
    assert str(Q.any(text=['search', 'engine'])) == 'text:search text:engine'
    assert str(Q.all(term, text='search')) == '+text:lucene +text:search'
    assert str(Q.all(text=['search', 'engine'])) == '+text:search +text:engine'
    assert str(Q.disjunct(0.0, term, text='search')) == '(text:lucene | text:search)'
    assert str(Q.disjunct(0.1, text=['search', 'engine'])) == '(text:search | text:engine)~0.1'
    assert str(Q.prefix('text', 'lucene')) == 'text:lucene*'
    assert str(Q.range('text', 'start', 'stop')) == 'text:[start TO stop}'
    assert str(Q.range('text', 'start', 'stop', lower=False, upper=True)) == 'text:{start TO stop]'
    assert str(Q.phrase('text', 'search', 'engine', slop=2)) == 'text:"search engine"~2'
    assert str(Q.phrase('text', 'search', None, 'engine')) == 'text:"search ? engine"'
    assert str(Q.multiphrase('text', 'lucene', None, ('search', 'engine'))) == 'text:"lucene ? (search engine)"'
    wildcard = Q.wildcard('text', '*')
    assert str(wildcard) == 'text:*' and isinstance(wildcard, search.WildcardQuery)
    assert str(Q.fuzzy('text', 'lucene')) == 'text:lucene~2'
    assert str(Q.fuzzy('text', 'lucene', 1)) == 'text:lucene~1'
    assert str(Q.alldocs()) == '*:*'
    assert str(Q.regexp('text', '.*')) == 'text:/.*/'

    span = Q.span('text', 'lucene')
    assert str(span) == 'text:lucene'
    assert str(span[10:]) == 'spanPosRange(text:lucene, 10, 2147483647)'
    assert str(span[:10]) == 'spanPosRange(text:lucene, 0, 10)'
    assert str(Q.span(wildcard)) == 'SpanMultiTermQueryWrapper(text:*)'
    near = Q.near('text', 'lucene', ('alias', 'search'), slop=-1, inOrder=False)
    assert str(near) == 'spanNear([text:lucene, mask(alias:search) as text], -1, false)'
    assert str(span - near) == 'spanNot(text:lucene, spanNear([text:lucene, mask(alias:search) as text], -1, false), 0, 0)'
    assert str(span | near) == 'spanOr([text:lucene, spanNear([text:lucene, mask(alias:search) as text], -1, false)])'
    assert str(span.mask('alias')) == 'mask(text:lucene) as alias'
    assert str(span.payload('')) == 'spanPayCheck(text:lucene, payloadRef: ;)'
    assert str(near.payload()) == 'spanPayCheck(spanNear([text:lucene, mask(alias:search) as text], -1, false), payloadRef: )'


def test_grouping(tempdir, indexer, zipcodes):
    field = indexer.fields['location'] = engine.NestedField('state.county.city', docValueType='sorted')
    for doc in zipcodes:
        if doc['state'] in ('CA', 'AK', 'WY', 'PR'):
            lat, lng = ('{0:08.3f}'.format(doc.pop(l)) for l in ['latitude', 'longitude'])
            location = '.'.join(doc[name] for name in ['state', 'county', 'city'])
            indexer.add(doc, latitude=lat, longitude=lng, location=location)
    indexer.commit()
    states = list(indexer.terms('state'))
    assert states[0] == 'AK' and states[-1] == 'WY'
    counties = [term.split('.')[-1] for term in indexer.terms('state.county', 'CA', 'CA~')]
    hits = indexer.search(field.prefix('CA'))
    assert sorted({hit['county'] for hit in hits}) == counties
    assert counties[0] == 'Alameda' and counties[-1] == 'Yuba'
    cities = [term.split('.')[-1] for term in indexer.terms('state.county.city', 'CA.Los Angeles', 'CA.Los Angeles~')]
    hits = indexer.search(field.prefix('CA.Los Angeles'))
    assert sorted({hit['city'] for hit in hits}) == cities
    assert cities[0] == 'Acton' and cities[-1] == 'Woodland Hills'
    hit, = indexer.search('zipcode:90210')
    assert hit['state'] == 'CA' and hit['county'] == 'Los Angeles' and hit['city'] == 'Beverly Hills' and hit['longitude'] == '-118.406'
    query = Q.prefix('zipcode', '90')
    (field, facets), = indexer.facets(query, 'state.county').items()
    assert field == 'state.county'
    la, orange = sorted(filter(facets.get, facets))
    assert la == 'CA.Los Angeles' and facets[la] > 100
    assert orange == 'CA.Orange' and facets[orange] > 10
    queries = {term: Q.term(field, term) for term in indexer.terms(field, 'CA.')}
    (field, facets), = indexer.facets(query, **{field: queries}).items()
    assert all(value.startswith('CA.') for value in facets) and set(facets) == set(queries)
    assert facets['CA.Los Angeles'] == 264
    groups = indexer.groupby(field, Q.term('state', 'CA'), count=1)
    assert len(groups) == 1 < groups.count
    hits, = groups
    assert hits.value == 'CA.Los Angeles' and len(hits) == 1 and hits.count > 100
    grouping = engine.documents.GroupingSearch(field, sort=search.Sort(indexer.sortfield(field)), cache=False, allGroups=True)
    assert all(grouping.search(indexer.indexSearcher, Q.alldocs()).facets.values())
    assert len(grouping) == len(list(grouping)) > 100
    assert set(grouping) > set(facets)
    hits = indexer.search(query, timeout=-1)
    assert not hits and (hits.count is hits.maxscore is None)
    hits = indexer.search(query, timeout=10)
    assert len(hits) == hits.count == indexer.count(query) and hits.maxscore == 1.0
    directory = store.RAMDirectory()
    query = Q.term('state', 'CA')
    size = indexer.copy(directory, query)
    searcher = engine.IndexSearcher(directory)
    assert len(searcher) == size and list(searcher.terms('state')) == ['CA']
    path = os.path.join(tempdir, 'temp')
    size = indexer.copy(path, exclude=query, merge=1)
    assert len(searcher) + size == len(indexer)
    searcher = engine.IndexSearcher(path)
    assert len(searcher.segments) == 1 and 'CA' not in searcher.terms('state')
    directory.close()


def test_spatial(indexer, zipcodes):
    point = engine.spatial.Point(-120, 39)
    assert point.coords == (-120, 39) and not list(point.within(1, 0))
    for name in ('longitude', 'latitude'):
        indexer.set(name, engine.NumericField, numericType=float, stored=True, docValueType='numeric')
    field = indexer.set('tile', engine.PointField, precision=15, numericPrecisionStep=2, stored=True)
    points = []
    for doc in zipcodes:
        if doc['state'] == 'CA':
            point = doc['longitude'], doc['latitude']
            indexer.add(doc, tile=[point])
            if doc['city'] == 'Los Angeles':
                points.append(point)
    assert len(list(engine.PolygonField('', precision=15).items(points))) > len(points)
    indexer.commit()
    city, zipcode, tile = 'Beverly Hills', '90210', '023012311120332'
    hit, = indexer.search('zipcode:' + zipcode)
    assert hit['tile'] == int(tile, 4) and hit['city'] == city
    hit, = indexer.search(field.prefix(tile))
    assert hit['zipcode'] == zipcode and hit['city'] == city
    x, y = (float(hit[l]) for l in ['longitude', 'latitude'])
    assert engine.spatial.Tile(2, 9, 4) == tile[:4]
    hits = indexer.search(field.near(x, y))
    cities = {hit['city'] for hit in hits}
    assert {city} == cities
    hits = indexer.search(field.near(x, y, precision=10))
    cities = {hit['city'] for hit in hits}
    assert city in cities and len(cities) > 10
    query = field.within(x, y, 10 ** 4)
    assert len(list(query)) == 3
    distances = indexer.distances(x, y, 'longitude', 'latitude')
    hits = indexer.search(query).sorted(distances.__getitem__)
    assert hits[0]['zipcode'] == zipcode and distances[hits[0].id] < 10
    cities = {hit['city'] for hit in hits}
    assert city in cities and len(cities) == 37
    hits = indexer.search(field.within(x, y, 10 ** 5, limit=100))
    cities = {hit['city'] for hit in hits}
    assert city in cities and len(cities) > 100
    ranges = 10 ** 2, 10 ** 5
    groups = hits.groupby(lambda id: bisect.bisect_left(ranges, distances[id]))
    counts = {hits.value: len(hits) for hits in groups}
    assert 1 == counts[0] < counts[2] < counts[1]
    assert len(list(field.within(x, y, 10 ** 8))) == 1
    hits = hits.filter(lambda id: distances[id] < 10 ** 4)
    assert 0 < len(hits) < sum(counts.values())
    hits = hits.sorted(distances.__getitem__, reverse=True)
    ids = list(hits.ids)
    assert 0 == distances[ids[-1]] < distances[ids[0]] < 10 ** 4


def test_fields(indexer, constitution):
    with pytest.raises(lucene.InvalidArgsError):
        engine.Field('', stored='invalid')
    with pytest.raises(lucene.JavaError):
        with engine.queries.suppress(search.TimeLimitingCollector.TimeExceededException):
            document.Field('name', 'value', document.FieldType())
    assert str(engine.Field.String('')) == str(document.StringField('', '', document.Field.Store.NO).fieldType())
    assert str(engine.Field.Text('')) == str(document.TextField('', '', document.Field.Store.NO).fieldType())
    assert str(engine.NumericField('', int)) == str(document.LongField('', 0L, document.Field.Store.NO).fieldType())
    assert str(engine.DateTimeField('')) == str(document.DoubleField('', 0.0, document.Field.Store.NO).fieldType())
    settings = {'docValueType': 'NUMERIC', 'indexOptions': 'DOCS_ONLY'}
    field = engine.Field('', **settings)
    assert field.settings == engine.Field('', **field.settings).settings == settings
    field = engine.NestedField('', stored=True)
    assert field.settings == {'stored': True, 'indexed': True, 'tokenized': False, 'omitNorms': True, 'indexOptions': 'DOCS_ONLY'}
    field = engine.Field('', indexed=True, omitNorms=True, storeTermVectors=True, storeTermVectorPositions=True, storeTermVectorOffsets=True)
    field, = field.items(' ')
    attrs = 'indexed', 'tokenized', 'storeTermVectors', 'storeTermVectorPositions', 'storeTermVectorOffsets', 'omitNorms'
    assert all(getattr(field.fieldType(), attr)() for attr in attrs)
    indexer.set('amendment', engine.Field.String, stored=True)
    indexer.set('size', engine.Field.String, stored=True, docValueType='sorted')
    field = indexer.fields['date'] = engine.NestedField('Y-m-d', sep='-', stored=True)
    for doc in constitution:
        if 'amendment' in doc:
            indexer.add(amendment='{:02}'.format(int(doc['amendment'])), date=doc['date'], size='{:04}'.format(len(doc['text'])))
    indexer.commit()
    assert set(indexer.fieldinfos) == {'amendment', 'Y', 'Y-m', 'Y-m-d', 'size'}
    assert str(indexer.fieldinfos['amendment'].indexOptions) == 'DOCS_ONLY'
    query = Q.range('amendment', '', '10')
    assert indexer.count(query) == 9
    query = Q.prefix('amendment', '0')
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
    sizes = {id: int(indexer[id]['size']) for id in indexer}
    ids = sorted((id for id in sizes if sizes[id] >= 1000), key=sizes.get)
    query = Q.range('size', '1000', None)
    hits = indexer.search(query).sorted(sizes.get)
    assert list(hits.ids) == ids
    hits = indexer.search(query, count=3, sort='size')
    assert list(hits.ids) == ids[:len(hits)]
    query = Q.range('size', None, '1000')
    assert indexer.count(query) == len(sizes) - len(ids)


def test_numeric(indexer, constitution):
    indexer.set('amendment', engine.NumericField, numericType=int, stored=True)
    field = indexer.set('date', engine.DateTimeField, stored=True)
    indexer.set('size', engine.NumericField, numericType=int, stored=True, numericPrecisionStep=5, docValueType='numeric')
    for doc in constitution:
        if 'amendment' in doc:
            indexer.add(amendment=int(doc['amendment']), date=[tuple(map(int, doc['date'].split('-')))], size=len(doc['text']))
    indexer.commit()
    query = field.prefix((1791, 12))
    assert indexer.count(query) == 10
    query = field.prefix(datetime.date(1791, 12, 15))
    assert indexer.count(query) == 10
    query = field.range(None, (1921, 12), lower=False, upper=True)
    assert indexer.count(query) == 19
    query = field.range(datetime.date(1919, 1, 1), datetime.date(1921, 12, 31))
    hits = indexer.search(query)
    assert [hit['amendment'] for hit in hits] == [18, 19]
    assert [datetime.datetime.utcfromtimestamp(float(hit['date'])).year for hit in hits] == [1919, 1920]
    assert indexer.count(field.within(seconds=100)) == indexer.count(field.within(weeks=1)) == 0
    query = field.duration([2009], days=-100 * 365)
    assert indexer.count(query) == 12
    field = indexer.fields['size']
    assert len(list(indexer.terms('size'))) > len(indexer)
    sizes = {id: int(indexer[id]['size']) for id in indexer}
    ids = sorted((id for id in sizes if sizes[id] >= 1000), key=sizes.get)
    query = field.range(1000, None)
    hits = indexer.search(query).sorted(sizes.get)
    assert list(hits.ids) == ids
    hits = indexer.search(query, count=3, sort=indexer.sortfield('size', type=int))
    assert list(hits.ids) == ids[:len(hits)]
    query = field.range(None, 1000)
    assert indexer.count(query) == len(sizes) - len(ids)
    assert str(field.range(-2 ** 64, 0)) == 'size:[* TO 0}'
    assert str(field.range(0, 2 ** 64)) == 'size:[0 TO *}'
    assert str(field.range(0.5, None, upper=True)) == 'size:[0.5 TO *]'
    for step, count in zip(range(0, 20, field.numericPrecisionStep), (26, 19, 3, 1)):
        sizes = list(indexer.numbers('size', step))
        assert len(sizes) == count and all(isinstance(size, int) for size in sizes)
        numbers = dict(indexer.numbers('size', step, type=float, counts=True))
        assert sum(numbers.values()) == len(indexer) and all(isinstance(number, float) for number in numbers)
    hit, = indexer.search(indexer.fields['amendment'].term(1))
    assert hit['amendment'] == 1


def test_highlighting(constitution):
    indexer = engine.Indexer()
    indexer.set('text', engine.Field.Text, stored=True, storeTermVectors=True, storeTermVectorPositions=True, storeTermVectorOffsets=True)
    for doc in constitution:
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
    assert len(fragments) == 2 and fragments[0].count('*') == 2 * 3 and '*persons*' in fragments[1]
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
    query = indexer.parse(query, field=text)
    highlighter = engine.queries.Highlighter(indexer.indexSearcher, query, 'text', terms=True, fields=True, tag='em')
    fragment, = highlighter.fragments(id)
    assert fragment and '<em>' in fragment


def test_nrt():
    indexer = engine.Indexer(nrt=True)
    indexer.add()
    assert indexer.count() == 0 and not indexer.current
    indexer.refresh()
    assert indexer.count() == 1 and indexer.current
    searcher = engine.IndexSearcher(indexer.directory)
    assert searcher.count() == 0 and searcher.current
    indexer.add()
    indexer.commit()
    assert indexer.count() == engine.IndexSearcher(indexer.directory).count() == 2


def test_multi(tempdir):
    indexers = engine.Indexer(tempdir), engine.Indexer()
    searcher = engine.MultiSearcher([indexers[0].indexReader, indexers[1].directory])
    pytest.raises(TypeError, getattr, searcher, 'timestamp')
    assert engine.MultiSearcher([indexers[0].directory]).timestamp
    assert [reader.refCount for reader in searcher.indexReaders] == [2, 1]
    assert searcher.reopen() is searcher
    indexers[0].add()
    indexers[0].commit()
    assert [reader.refCount for reader in searcher.indexReaders] == [1, 1]
    searcher, previous = searcher.reopen(), searcher
    assert searcher.version > previous.version
    assert [reader.refCount for reader in searcher.indexReaders] == [1, 2]
    del previous
    assert [reader.refCount for reader in searcher.indexReaders] == [1, 1]


def test_docvalues():
    indexer = engine.Indexer()
    indexer.set('id', engine.Field.String)
    indexer.set('title', docValueType='binary')
    indexer.set('size', docValueType='numeric')
    indexer.set('point', docValueType='numeric')
    indexer.set('priority', docValueType='sorted')
    indexer.set('tags', docValueType='sorted_set')
    indexer.set('sizes', docValueType='sorted_numeric')
    indexer.set('points', docValueType='sorted_numeric')
    indexer.add(id='0', title='zero', size=0, point=0.5, priority='low', tags=['red'], sizes=[0], points=[0.5])
    indexer.commit()

    with pytest.raises(AttributeError):
        indexer.sortfield('id')
    sortfield = indexer.sortfield('id', type='string', reverse=True)
    assert sortfield.field == 'id' and sortfield.reverse and sortfield.type == search.SortField.Type.STRING
    sortfield = indexer.sortfield('title')
    assert sortfield.field == 'title' and not sortfield.reverse and sortfield.type == search.SortField.Type.BYTES
    assert indexer.sortfield('size', type=int).type == search.SortField.Type.LONG
    assert indexer.sortfield('point', type=float).type == search.SortField.Type.DOUBLE
    assert indexer.sortfield('priority').type == search.SortField.Type.STRING
    assert indexer.sortfield('tags').type == search.SortField.Type.STRING
    assert indexer.sortfield('sizes').type == search.SortField.Type.LONG
    assert indexer.sortfield('points', type=float).type == search.SortField.Type.DOUBLE

    segments = indexer.segments
    indexer.update('id', id='0', title='one', size=1, point=1.5, priority='high', tags=['blue'], sizes=[1], points=[1.5])
    indexer.commit()
    assert indexer.segments != segments
    segments = indexer.segments
    docvalues = indexer.docvalues('title')
    assert len(docvalues) == 1 and docvalues[0] == 'one'
    assert list(indexer.docvalues('size', type=int)) == [1]
    assert list(indexer.docvalues('point', type=float)) == [1.5]
    assert list(indexer.docvalues('priority')) == ['high']
    assert list(indexer.docvalues('tags')) == [('blue',)]
    assert list(indexer.docvalues('sizes', type=int)) == [(1,)]
    assert list(indexer.docvalues('points', type=float)) == [(1.5,)]
    indexer.update('id', '0', title='two', size=2, point=2.5)
    indexer.update('id', '0')
    indexer.commit()
    assert indexer.segments == segments
    assert list(indexer.docvalues('title')) == ['two']
    assert list(indexer.docvalues('size', type=int)) == [2]
    assert list(indexer.docvalues('point', type=float)) == [2.5]
    with pytest.raises(AttributeError):
        indexer.docvalues('id')
