import datetime
import json
import math
from pathlib import Path
import pytest
import lucene
from org.apache.lucene import analysis, document, geo, search, store, util
from lupyne import engine

Q = engine.Query


class typeAsPayload(engine.TokenFilter):
    "Custom implementation of lucene TypeAsPayloadTokenFilter."

    def incrementToken(self):
        result = self.input.incrementToken()
        self.payload = self.type
        return result


@pytest.fixture
def indexer(tempdir):
    with engine.Indexer(tempdir) as indexer:
        for name in ('city', 'county', 'state', 'latitude', 'longitude'):
            indexer.set(name, stored=True)
        indexer.set('zipcode', engine.Field.String, stored=True)
        yield indexer


def test_analyzers(tempdir):
    stemmer = engine.Analyzer.standard(analysis.en.PorterStemFilter, typeAsPayload)
    for token in stemmer.tokens('Search'):
        assert token.positionIncrement == 1
        assert engine.TokenFilter(analysis.miscellaneous.EmptyTokenStream()).payload is None
        assert token.charTerm == 'search'
        assert token.type == token.payload == '<ALPHANUM>'
        assert token.offset == (0, 6)
        token.charTerm = token.type = ''
        token.offset, token.positionIncrement = (0, 0), 0
    assert str(stemmer.parse('searches', field=['body', 'title'])) == 'body:search title:search'
    assert (
        str(stemmer.parse('searches', field={'body': 1.0, 'title': 2.0}))
        == '(body:search)^1.0 (title:search)^2.0'
    )
    indexer = engine.Indexer(tempdir, analyzer=stemmer)
    indexer.set('text', engine.Field.Text)
    indexer.add(text='searches')
    indexer.commit()
    (item,) = indexer.positions('text', 'search', payloads=True)
    assert item == (0, [(0, '<ALPHANUM>')])
    analyzer = engine.Analyzer.whitespace(engine.TokenFilter)
    assert [token.charTerm for token in analyzer.tokens('Search Engine')] == ['Search', 'Engine']


def test_writer(tempdir):
    indexer = engine.Indexer(tempdir, useCompoundFile=False)
    assert not indexer.config.useCompoundFile
    with pytest.raises(lucene.JavaError):
        engine.Indexer(indexer.directory)
    indexer.set('text', engine.Field.Text)
    indexer.set('name', stored=True)
    indexer.set('tag', engine.Field.Text, stored=True)
    searcher = indexer.indexSearcher
    indexer.commit()
    assert searcher is indexer.indexSearcher
    assert not searcher.search(count=1)
    indexer.add(text='hello world', name='sample', tag=['python', 'search'])
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
    assert list(indexer.docs('text', 'hello')) == [0]
    assert list(indexer.docs('text', 'hi')) == []
    assert list(indexer.docs('text', 'world', counts=True)) == [(0, 1)]
    assert list(indexer.positions('text', 'world')) == [(0, [1])]
    assert list(indexer.positions('text', 'world', offsets=True)) == [(0, [(-1, -1)])]
    hits = indexer.search('text:hello')
    assert len(hits) == hits.count == 1
    assert hits.scoredocs == hits[:1].scoredocs and not hits[1:]
    assert list(hits.ids) == [0]
    (score,) = hits.scores
    assert 0 < score < 1
    assert dict(hits.items()) == {0: score}
    data = hits[0].dict()
    assert data['__id__'] == 0 and '__score__' in data
    assert not indexer.search('hello') and indexer.search('hello', field='text')
    assert indexer.search('text:hello hi') and not indexer.search('text:hello hi', op='and')
    assert indexer.search('text:*hello', allowLeadingWildcard=True)
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


def test_searcher(tempdir, fields, constitution):
    indexer = engine.Indexer(tempdir)
    indexer.fields = {field.name: field for field in fields}
    for doc in constitution:
        indexer.add(doc)
    indexer.commit()
    assert indexer.complete('amendment', '', 1)
    indexer.delete('amendment', doc['amendment'])
    indexer.add(doc)
    reader = indexer.indexReader
    indexer.commit()
    assert reader.refCount == 0
    analyzer = engine.Analyzer.standard()
    doc = {'text': doc['text'], 'amendment': analyzer.tokens(doc['amendment'])}
    scores = list(indexer.match(doc, 'text:congress', 'text:law', 'amendment:27'))
    assert 0.0 == scores[0] < scores[1] <= scores[2] < 1.0
    assert len(indexer) == len(indexer.search()) == 35
    articles = list(indexer.terms('article'))
    articles.remove('Preamble')
    assert sorted(map(int, articles)) == list(range(1, 8))
    assert sorted(map(int, indexer.terms('amendment'))) == list(range(1, 28))
    assert list(indexer.terms('text', 'right')) == ['right', 'rights']
    assert dict(indexer.terms('text', 'right', counts=True)) == {'right': 13, 'rights': 1}
    assert list(indexer.terms('text', 'right', 'right_')) == ['right']
    assert dict(indexer.terms('text', 'right', 'right_', counts=True)) == {'right': 13}
    assert list(indexer.terms('text', 'right', distance=1)) == ['eight', 'right', 'rights']
    assert dict(indexer.terms('text', 'right', distance=1, counts=True)) == {
        'eight': 3,
        'right': 13,
        'rights': 1,
    }
    assert list(indexer.terms('text', 'senite', distance=2)) == ['senate', 'sent']
    word, count = next(indexer.terms('text', 'people', counts=True))
    assert word == 'people' and count == 8
    docs = dict(indexer.docs('text', 'people', counts=True))
    counts = list(docs.values())
    assert len(docs) == count and all(counts) and sum(counts) > count
    positions = dict(indexer.positions('text', 'people'))
    assert list(map(len, positions.values())) == counts
    (hit,) = indexer.search('"We the People"', field='text')
    assert hit['article'] == 'Preamble'
    assert sorted(hit.dict()) == ['__id__', '__score__', 'article']
    hits = indexer.search('people', field='text')
    assert 'Preamble' in (hit.get('article') for hit in hits)
    assert len(hits) == hits.count == 8
    assert set(map(type, hits.ids)) == {int} and set(map(type, hits.scores)) == {float}
    assert hits.maxscore == next(hits.scores)
    ids = list(hits.ids)
    hits = indexer.search('people', count=5, mincount=5, field='text')
    assert list(hits.ids) == ids[: len(hits)]
    assert len(hits) == 5 and hits.count == 8
    assert not any(map(math.isnan, hits.scores))
    assert hits.maxscore == next(hits.scores)
    hits = indexer.search('text:people', count=5, sort=search.Sort.INDEXORDER, scores=True)
    assert sorted(hits.ids) == list(hits.ids)
    assert all(score > 0 for score in hits.scores)
    (hit,) = indexer.search('freedom', field='text')
    assert hit['amendment'] == '1'
    assert sorted(hit.dict()) == ['__id__', '__score__', 'amendment', 'date']
    hits = indexer.search('date:[1919 TO 1921]')
    amendments = ['18', '19']
    assert sorted(hit['amendment'] for hit in hits) == amendments
    query = Q.range('date', '1919', '1921')
    span = Q.span('text', 'persons')
    count = indexer.count(span)
    spans = dict(indexer.spans(span))
    assert len(spans) == count and spans == dict(indexer.docs('text', 'persons', counts=True))
    near = Q.near('text', 'persons', 'papers', slop=2)
    ((id, positions),) = indexer.spans(near, positions=True)
    assert indexer[id]['amendment'] == '4' and positions in ([(3, 6)], [(10, 13)])
    assert 'persons' in indexer.termvector(id, 'text')
    assert dict(indexer.termvector(id, 'text', counts=True))['persons'] == 2
    assert dict(indexer.positionvector(id, 'text'))['persons'] in ([3, 26], [10, 48])
    assert dict(indexer.positionvector(id, 'text', offsets=True))['persons'] == [
        (46, 53),
        (301, 308),
    ]
    analyzer = analysis.core.WhitespaceAnalyzer()
    query = indexer.morelikethis(0, analyzer=analyzer)
    assert {'text:united', 'text:states'} <= set(str(query).split())
    assert str(indexer.morelikethis(0, 'article', analyzer=analyzer)) == ''
    query = indexer.morelikethis(0, minDocFreq=3, analyzer=analyzer)
    assert {'text:establish', 'text:united', 'text:states'} <= set(str(query).split())
    assert (
        str(indexer.morelikethis('jury', 'text', minDocFreq=4, minTermFreq=1, analyzer=analyzer))
        == 'text:jury'
    )
    assert str(indexer.morelikethis('jury', 'article', analyzer=analyzer)) == ''


def test_spellcheck(tempdir, fields, constitution):
    indexer = engine.Indexer(tempdir)
    indexer.fields = {field.name: field for field in fields}
    for doc in constitution:
        indexer.add(doc)
    indexer.commit()
    assert indexer.dictionary('text')
    assert indexer.dictionary('text', 0.5)
    assert indexer.complete('missing', '', 1) == []
    assert ['the', 'shall'] == indexer.complete('text', '', 2)
    assert indexer.complete('text', 'con', 2) == ['congress', 'constitution']
    assert indexer.complete('text', 'congress', 2) == ['congress']
    assert indexer.complete('text', 'congresses', 1) == []
    assert indexer.suggest('text', 'write') == ['writs']
    assert indexer.suggest('text', 'write', 3) == ['writs', 'writ', 'written']
    assert indexer.suggest('text', 'write', 3, maxEdits=1) == ['writs', 'writ']
    query = indexer.parse('text:write', spellcheck=True)
    assert search.TermQuery.instance_(query) and str(query) == 'text:writs'
    query = indexer.parse('"hello world"', field='text', spellcheck=True)
    assert search.PhraseQuery.instance_(query) and str(query) == 'text:"held would"'
    assert str(indexer.parse('vwxyz', field='text', spellcheck=True)) == 'text:vwxyz'


def test_indexes(tempdir):
    with pytest.raises(lucene.JavaError):
        engine.Indexer(tempdir, 'r')
    indexer = engine.Indexer(tempdir)
    indexer.set('name', engine.Field.String, stored=True)
    indexer.set('text', engine.Field.Text)
    with engine.Indexer(tempdir + '/other') as temp:
        temp.add()
    with pytest.raises(KeyError), engine.Indexer(tempdir + '/other') as temp:
        raise KeyError
    for other in (temp, temp.directory):
        indexer += other
    assert len(indexer) == 2
    indexer.add()
    analyzer = engine.Analyzer.whitespace()
    indexer.add(text=analyzer.tokens('?'), name=util.BytesRef('{}'))
    indexer.commit()
    assert indexer[next(indexer.docs('text', '?'))]['name'] == '{}'
    indexer.delete('text', '?')
    indexer.commit(merge=True)
    indexer.commit(merge=1)
    assert len(list(indexer.readers)) == 1
    reader = engine.indexers.IndexReader(indexer.indexReader)
    del reader.indexReader
    with pytest.raises(AttributeError):
        reader.maxDoc
    del indexer.indexSearcher
    with pytest.raises(AttributeError):
        indexer.search
    indexer.close()

    indexer = engine.Indexer(tempdir)
    indexer.add()
    path = Path(tempdir, 'temp')
    with indexer.snapshot() as commit:
        indexer.commit(merge=1)
        assert indexer.indexCommit.generation > commit.generation
        engine.indexers.copy(commit, path)
        filepath = path / commit.segmentsFileName
        filepath.unlink()
        open(filepath, 'w').close()
        with pytest.raises(OSError):
            engine.indexers.copy(commit, path)
    with pytest.raises(lucene.JavaError):
        indexer.check(tempdir)


def test_check(tempdir):
    engine.Indexer(tempdir).close()
    assert engine.IndexWriter.check(tempdir).clean
    assert not engine.IndexWriter.check(tempdir, repair=True).numBadSegments


def test_queries():
    alldocs = search.MatchAllDocsQuery()
    term = Q.term('text', 'lucene')
    assert str(term) == 'text:lucene'
    assert str(term.constant()) == 'ConstantScore(text:lucene)'
    assert str(term.boost(2.0)) == '(text:lucene)^2.0'
    assert str(+term) == '+text:lucene'
    assert str(-term) == '-text:lucene'
    assert str(term & alldocs) == '+text:lucene +*:*'
    assert str(alldocs & term) == '+*:* +text:lucene'
    assert str(term | alldocs) == 'text:lucene *:*'
    assert str(alldocs | term) == '*:* text:lucene'
    assert str(term - alldocs) == 'text:lucene -*:*'
    assert str(alldocs - term) == '*:* -text:lucene'

    terms = str(Q.terms('text', ['search', 'engine']))
    assert terms.startswith('text:') and 'search' in terms and 'engine' in terms
    assert str(Q.any(term, text='search')) == 'text:lucene text:search'
    assert str(Q.any(text=['search', 'engine'])) == 'text:search text:engine'
    assert str(Q.all(term, text='search')) == '+text:lucene +text:search'
    assert str(Q.all(text=['search', 'engine'])) == '+text:search +text:engine'
    assert str(Q.filter(term, text='search')) == '#text:lucene #text:search'
    assert str(Q.filter(text=['search', 'engine'])) == '#text:search #text:engine'
    assert str(Q.disjunct(0.0, term, text='search')).startswith('(text:')
    assert str(Q.disjunct(0.1, text=['search', 'engine'])).endswith('~0.1')
    assert str(Q.prefix('text', 'lucene')) == 'text:lucene*'
    assert str(Q.range('text', 'start', 'stop')) == 'text:[start TO stop}'
    assert str(Q.range('text', 'start', 'stop', lower=False, upper=True)) == 'text:{start TO stop]'
    assert str(Q.phrase('text', 'search', 'engine', slop=2)) == 'text:"search engine"~2'
    assert str(Q.phrase('text', 'search', None, 'engine')) == 'text:"search ? engine"'
    assert str(Q.phrase('text', 'lucene', ('search', 'engine'))) == 'text:"lucene (search engine)"'
    wildcard = Q.wildcard('text', '*')
    assert str(wildcard) == 'text:*' and isinstance(wildcard, search.WildcardQuery)
    assert str(Q.fuzzy('text', 'lucene')) == 'text:lucene~2'
    assert str(Q.fuzzy('text', 'lucene', 1)) == 'text:lucene~1'
    assert str(Q.alldocs()) == '*:*'
    assert str(Q.nodocs()) == 'MatchNoDocsQuery("")'
    assert str(Q.regexp('text', '.*')) == 'text:/.*/'

    span = Q.span('text', 'lucene')
    assert str(span) == 'text:lucene'
    assert str(span[10:]) == 'spanPosRange(text:lucene, 10, 2147483647)'
    assert str(span[:10]) == 'spanPosRange(text:lucene, 0, 10)'
    assert str(Q.span(wildcard)) == 'SpanMultiTermQueryWrapper(text:*)'
    near = Q.near('text', 'lucene', ('alias', 'search'), slop=-1, inOrder=False)
    assert str(near) == 'spanNear([text:lucene, mask(alias:search) as text], -1, false)'
    assert (
        str(span - near)
        == 'spanNot(text:lucene, spanNear([text:lucene, mask(alias:search) as text], -1, false), 0, 0)'
    )
    assert (
        str(span | near)
        == 'spanOr([text:lucene, spanNear([text:lucene, mask(alias:search) as text], -1, false)])'
    )
    assert str(span.mask('alias')) == 'mask(text:lucene) as alias'
    assert str(span.boost(2.0)) == '(text:lucene)^2.0'
    assert str(span.containing(span)) == 'SpanContaining(text:lucene, text:lucene)'
    assert str(span.within(span)) == 'SpanWithin(text:lucene, text:lucene)'

    assert str(Q.points('point', 0.0)) == 'point:{0.0}'
    assert str(Q.points('point', 0.0, 1.0)) == 'point:{0.0 1.0}'
    assert str(Q.points('point', 0)) == 'point:{0}'
    assert str(Q.points('point', 0, 1)) == 'point:{0 1}'
    assert (
        str(Q.ranges('point', (0.0, 1.0), (2.0, 3.0), upper=True))
        == 'point:[0.0 TO 1.0],[2.0 TO 3.0]'
    )
    assert str(Q.ranges('point', (0.0, 1.0), lower=False)).startswith('point:[4.9E-324 TO 0.9999')
    assert str(Q.ranges('point', (None, 0.0), upper=True)) == 'point:[-Infinity TO 0.0]'
    assert str(Q.ranges('point', (0.0, None))) == 'point:[0.0 TO Infinity]'
    assert str(Q.ranges('point', (0, 1), (2, 3), upper=True)) == 'point:[0 TO 1],[2 TO 3]'
    assert str(Q.ranges('point', (0, 3), lower=False)) == 'point:[1 TO 2]'
    assert str(Q.ranges('point', (None, 0), upper=True)) == 'point:[-9223372036854775808 TO 0]'
    assert str(Q.ranges('point', (0, None))) == 'point:[0 TO 9223372036854775807]'


def test_grouping(tempdir, indexer, zipcodes):
    field = indexer.fields['location'] = engine.NestedField(
        'state.county.city', docValuesType='sorted'
    )
    for doc in zipcodes:
        if doc['state'] in ('CA', 'AK', 'WY', 'PR'):
            lat, lng = ('{0:08.3f}'.format(doc.pop(lt)) for lt in ['latitude', 'longitude'])
            location = '.'.join(doc[name] for name in ['state', 'county', 'city'])
            indexer.add(doc, latitude=lat, longitude=lng, location=location)
    indexer.commit()
    states = list(indexer.terms('state'))
    assert states[0] == 'AK' and states[-1] == 'WY'
    counties = [term.split('.')[-1] for term in indexer.terms('state.county', 'CA')]
    hits = indexer.search(field.prefix('CA'))
    assert sorted({hit['county'] for hit in hits}) == counties
    assert counties[0] == 'Alameda' and counties[-1] == 'Yuba'
    cities = [term.split('.')[-1] for term in indexer.terms('state.county.city', 'CA.Los Angeles')]
    hits = indexer.search(field.prefix('CA.Los Angeles'))
    assert sorted({hit['city'] for hit in hits}) == cities
    assert cities[0] == 'Acton' and cities[-1] == 'Woodland Hills'
    (hit,) = indexer.search('zipcode:90210')
    assert hit['state'] == 'CA' and hit['county'] == 'Los Angeles'
    assert hit['city'] == 'Beverly Hills' and hit['longitude'] == '-118.406'
    query = Q.prefix('zipcode', '90')
    ((field, facets),) = indexer.facets(query, 'state.county').items()
    assert field == 'state.county'
    la, orange = sorted(filter(facets.get, facets))
    assert la == 'CA.Los Angeles' and facets[la] > 100
    assert orange == 'CA.Orange' and facets[orange] > 10
    queries = {term: Q.term(field, term) for term in indexer.terms(field, 'CA.')}
    ((field, facets),) = indexer.facets(query, **{field: queries}).items()
    assert all(value.startswith('CA.') for value in facets) and set(facets) == set(queries)
    assert facets['CA.Los Angeles'] == 264
    groups = indexer.groupby(field, Q.term('state', 'CA'), count=1)
    assert len(groups) == 1 < groups.count
    (hits,) = groups
    assert hits.value == 'CA.Los Angeles' and len(hits) == 1 and hits.count > 100
    sort = search.Sort(indexer.sortfield(field))
    grouping = engine.documents.GroupingSearch(field, sort=sort, cache=False, allGroups=True)
    assert all(grouping.search(indexer.indexSearcher, Q.alldocs()).facets.values())
    assert len(grouping) == len(list(grouping)) > 100
    assert set(grouping) > set(facets)
    hits = indexer.search(query, timeout=-1)
    assert not hits and not hits.count and math.isnan(hits.maxscore)
    hits = indexer.search(query, timeout=10)
    assert len(hits) == hits.count == indexer.count(query) and hits.maxscore == 1.0
    directory = store.ByteBuffersDirectory()
    query = Q.term('state', 'CA')
    size = indexer.copy(directory, query)
    searcher = engine.IndexSearcher(directory)
    assert len(searcher) == size and list(searcher.terms('state')) == ['CA']
    path = str(Path(tempdir, 'temp'))
    size = indexer.copy(path, exclude=query, merge=1)
    assert len(searcher) + size == len(indexer)
    searcher = engine.IndexSearcher(path)
    assert len(searcher.segments) == 1 and 'CA' not in searcher.terms('state')
    directory.close()


def test_shape(indexer, zipcodes):
    for name in ('longitude', 'latitude'):
        indexer.set(name, dimensions=1, stored=True)
    latlon = indexer.set('latlon', engine.ShapeField)
    xy = indexer.set('xy', engine.ShapeField, indexed=False, docvalues=True)
    for doc in zipcodes:
        if doc['state'] == 'CA':
            point = doc['latitude'], doc['longitude']
            indexer.add(doc, latlon=geo.Point(*point), xy=geo.XYPoint(*point[::-1]))
    indexer.commit()
    assert indexer.count(latlon.contains(geo.Point(*point))) == 0
    assert indexer.count(latlon.disjoint(geo.Point(*point))) == 2647
    line = [point[0] - 1, point[0] + 1], [point[1], point[1]]
    assert indexer.count(latlon.disjoint(geo.Line(*line))) == 2647
    assert indexer.count(latlon.intersects(geo.Line(*line))) == 0
    circle = point[0], point[1], 1.0
    assert indexer.count(latlon.disjoint(geo.Circle(*circle))) == 2646
    assert indexer.count(latlon.intersects(geo.Circle(*circle))) == 1
    assert indexer.count(latlon.within(geo.Circle(*circle))) == 1
    lat, lon = point
    polygon = {
        'type': 'Polygon',
        'coordinates': [[(lon, lat), (lon + 1, lat), (lon, lat + 1), (lon, lat)]],
    }
    (pg,) = geo.Polygon.fromGeoJSON(json.dumps(polygon))
    assert indexer.count(latlon.contains(pg)) == 0
    assert indexer.count(latlon.disjoint(pg)) == 2639
    assert indexer.count(latlon.intersects(pg)) == 8
    hits = indexer.search(latlon.within(pg))
    assert len(hits) == 8
    counties = {hit.id: hit['county'] for hit in hits}
    groups = hits.groupby(counties.get)
    assert len(groups) == 4
    assert {group.value for group in groups} == set(counties.values())
    assert len(hits.filter(lambda id: counties[id] == 'Nevada')) == 3

    assert isinstance(xy.contains(geo.XYPoint(*point)), search.Query)
    assert isinstance(xy.disjoint(geo.XYLine(*line)), search.Query)
    assert isinstance(xy.intersects(geo.XYCircle(*circle)), search.Query)
    assert isinstance(latlon.distances(geo.Point(*point)), search.SortField)
    assert isinstance(xy.distances(geo.XYPoint(*point)), search.SortField)
    (field,) = latlon.items(pg)
    assert isinstance(field, document.Field)


def test_fields(indexer, constitution):
    with pytest.raises(lucene.InvalidArgsError):
        engine.Field('', stored='invalid')
    with pytest.raises(AttributeError):
        engine.Field('', invalid=None)
    with pytest.raises(lucene.JavaError):
        with engine.utils.suppress(search.TimeLimitingCollector.TimeExceededException):
            document.Field('name', 'value', document.FieldType())
    assert str(engine.Field.String('')) == str(
        document.StringField('', '', document.Field.Store.NO).fieldType()
    )
    assert str(engine.Field.Text('')) == str(
        document.TextField('', '', document.Field.Store.NO).fieldType()
    )
    assert str(engine.DateTimeField('')) == str(document.DoublePoint('', 0.0).fieldType())
    settings = {'docValuesType': 'NUMERIC', 'indexOptions': 'DOCS'}
    field = engine.Field('', **settings)
    assert field.settings == engine.Field('', **field.settings).settings == settings
    field = engine.NestedField('', stored=True)
    assert field.settings == {
        'stored': True,
        'tokenized': False,
        'omitNorms': True,
        'indexOptions': 'DOCS',
    }
    attrs = (
        'stored',
        'omitNorms',
        'storeTermVectors',
        'storeTermVectorPositions',
        'storeTermVectorOffsets',
    )
    field = engine.Field('', indexOptions='docs', **dict.fromkeys(attrs, True))
    (field,) = field.items(' ')
    assert all(getattr(field.fieldType(), attr)() for attr in attrs)
    indexer.set('amendment', engine.Field.String, stored=True)
    indexer.set('size', engine.Field.String, stored=True, docValuesType='sorted')
    field = indexer.fields['date'] = engine.NestedField('Y-m-d', sep='-', stored=True)
    for doc in constitution:
        if 'amendment' in doc:
            indexer.add(
                amendment='{:02}'.format(int(doc['amendment'])),
                date=doc['date'],
                size='{:04}'.format(len(doc['text'])),
            )
    indexer.commit()
    assert set(indexer.fieldinfos) == {'amendment', 'Y', 'Y-m', 'Y-m-d', 'size'}
    assert str(indexer.fieldinfos['amendment'].indexOptions) == 'DOCS'
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
    assert list(hits.ids) == ids[: len(hits)]
    hits.select('amendment')
    hit = hits[0].dict()
    assert math.isnan(hit.pop('__score__'))
    assert hit == {'amendment': '20', '__id__': 19, '__sortkeys__': ('1923',)}
    query = Q.range('size', None, '1000')
    assert indexer.count(query) == len(sizes) - len(ids)


def test_numeric(indexer, constitution):
    indexer.set('amendment', dimensions=1, stored=True)
    field = indexer.set('date', engine.DateTimeField, stored=True)
    indexer.set('size', dimensions=1, stored=True, docValuesType='numeric')
    for doc in constitution:
        if 'amendment' in doc:
            indexer.add(
                amendment=int(doc['amendment']),
                date=[tuple(map(int, doc['date'].split('-')))],
                size=len(doc['text']),
            )
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
    dates = [datetime.datetime.fromtimestamp(float(hit['date'])).year for hit in hits]
    assert dates == [1919, 1920]
    assert indexer.count(field.within(seconds=100)) == indexer.count(field.within(weeks=1)) == 0
    query = field.duration([2009], days=-100 * 365)
    assert indexer.count(query) == 12
    sizes = {id: indexer[id]['size'] for id in indexer}
    ids = sorted((id for id in sizes if sizes[id] >= 1000), key=sizes.get)
    query = Q.ranges('size', (1000, None))
    hits = indexer.search(query).sorted(sizes.get)
    assert list(hits.ids) == ids
    hits = indexer.search(query, count=3, sort=indexer.sortfield('size', type=int))
    assert list(hits.ids) == ids[: len(hits)]
    query = Q.ranges('size', (None, 1000))
    assert indexer.count(query) == len(sizes) - len(ids)
    (hit,) = indexer.search(Q.points('amendment', 1))
    assert hit['amendment'] == 1


def test_highlighting(tempdir, constitution):
    indexer = engine.Indexer(tempdir)
    indexer.set(
        'text',
        engine.Field.Text,
        stored=True,
        storeTermVectors=True,
        storeTermVectorPositions=True,
        storeTermVectorOffsets=True,
    )
    for doc in constitution:
        if 'amendment' in doc:
            indexer.add(text=doc['text'])
    indexer.commit()
    query = Q.term('text', 'right')
    assert (
        engine.Analyzer.highlight(indexer.analyzer, query, 'text', "word right word")
        == "word <b>right</b> word"
    )
    hits = indexer.search(query)
    highlights = list(hits.highlights(query, text=1))
    assert len(hits) == len(highlights)
    for highlight in highlights:
        assert '<b>right</b>' in highlight.pop('text') and not highlight


def test_nrt(tempdir):
    indexer = engine.Indexer(tempdir, nrt=True)
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
    indexers = engine.Indexer(tempdir), engine.Indexer(tempdir + '/other')
    searcher = engine.MultiSearcher([indexers[0].indexReader, indexers[1].directory])
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


def test_docvalues(tempdir):
    indexer = engine.Indexer(tempdir)
    indexer.set('id', engine.Field.String)
    indexer.set('title', docValuesType='binary')
    indexer.set('size', docValuesType='numeric')
    indexer.set('point', docValuesType='numeric')
    indexer.set('priority', docValuesType='sorted')
    indexer.set('tags', docValuesType='sorted_set')
    indexer.set('sizes', docValuesType='sorted_numeric')
    indexer.set('points', docValuesType='sorted_numeric')
    indexer.add(
        id='0',
        title='zero',
        size=0,
        point=0.5,
        priority='low',
        tags=['red'],
        sizes=[0],
        points=[0.5],
    )
    indexer.commit()

    with pytest.raises(AttributeError):
        indexer.sortfield('id')
    sortfield = indexer.sortfield('id', type='string', reverse=True)
    assert (
        sortfield.field == 'id'
        and sortfield.reverse
        and sortfield.type == search.SortField.Type.STRING
    )
    sortfield = indexer.sortfield('title')
    assert (
        sortfield.field == 'title'
        and not sortfield.reverse
        and sortfield.type == search.SortField.Type.STRING
    )
    assert indexer.sortfield('size', type=int).type == search.SortField.Type.LONG
    assert indexer.sortfield('point', type=float).type == search.SortField.Type.DOUBLE
    assert indexer.sortfield('priority').type == search.SortField.Type.STRING
    assert indexer.sortfield('tags').type == search.SortField.Type.STRING
    assert indexer.sortfield('sizes').type == search.SortField.Type.LONG
    assert indexer.sortfield('points', type=float).type == search.SortField.Type.DOUBLE

    segments = indexer.segments
    indexer.update(
        'id',
        id='0',
        title='one',
        size=1,
        point=1.5,
        priority='high',
        tags=['blue'],
        sizes=[1],
        points=[1.5],
    )
    indexer.commit()
    assert indexer.segments != segments
    segments = indexer.segments
    assert list(indexer.docvalues('title')) == ['one']
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
    assert indexer.search().docvalues('title') == {0: 'two'}

    indexer.add()
    indexer.commit()
    assert None in indexer.docvalues('title')
    assert None in indexer.docvalues('size', type=int)
    assert None in indexer.docvalues('tags')
    assert None in indexer.docvalues('sizes', type=int)
