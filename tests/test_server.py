import calendar
import httplib
import json
import math
import operator
import os
import signal
import subprocess
import sys
import time
import urllib
from email.utils import parsedate
import cherrypy
import clients
import portend
import pytest
from lupyne import engine, server


class Servers(dict):
    module = 'lupyne.server'
    ports = 8080, 8081, 8082
    config = {'server.socket_timeout': 2, 'server.shutdown_timeout': 1}

    def start(self, port, *args, **config):
        config.update(self.config)
        config['server.socket_port'] = port
        portend.free('localhost', port)
        server = self[port] = subprocess.Popen((sys.executable, '-m', self.module, '-c', json.dumps(config)) + args)
        portend.occupied('localhost', port)
        server.started = time.time()
        assert not server.poll()
        return clients.Resource('http://localhost:{}'.format(port))

    def stop(self, port):
        server = self.pop(port)
        time.sleep(max(0, server.started + 0.2 - time.time()))  # let server finish starting for clean shutdown
        server.terminate()
        assert server.wait() == 0


@pytest.fixture
def servers(request):
    servers = Servers()
    servers.config['log.screen'] = request.config.option.verbose > 0
    yield servers
    for port in list(servers):
        servers.stop(port)


@pytest.fixture
def resource(tempdir, servers, constitution):
    resource = servers.start(servers.ports[0], tempdir)
    for name, settings in constitution.fields.items():
        assert resource.client.put('fields/' + name, settings).status_code == httplib.CREATED
    assert resource.post('docs', list(constitution)) is None
    assert resource.post('update', {'spellcheckers': True, 'merge': 1})
    return resource


def test_docs(resource):
    (directory, size), = resource().items()
    assert 'Directory@' in directory and size == 35
    fields = resource.get('/fields')
    assert sorted(fields) == ['amendment', 'article', 'date', 'text']
    for field in fields:
        assert resource.fields(field)['indexed']
    assert resource.client.put('fields/' + field, {}).status_code == httplib.OK
    assert resource.docs('0', **{'fields.indexed': 'amendment:int'}) == {'amendment': 0, 'article': 'Preamble'}
    doc = resource.docs('0', **{'fields.vector': 'text,missing'})
    assert doc['missing'] == [] and doc['text'].index('states') < doc['text'].index('united')
    doc = resource.docs('0', **{'fields.vector.counts': 'text'})
    resource.client.patch('docs/amendment/1', {'amendment': '1'}).status_code == httplib.CONFLICT
    assert not resource.patch('docs/amendment/1')
    assert sorted(term for term, count in doc['text'].items() if count > 1) == ['establish', 'states', 'united']
    doc = resource.docs('amendment/1', **{'fields.multi': 'text', 'fields.indexed': 'text'})
    assert doc['text'][:3] == ['abridging', 'assemble', 'congress']
    assert resource.client.put('docs/article/0', {'article': '-1'}).status_code == httplib.BAD_REQUEST
    assert resource.delete('docs/article/0') is None


def test_terms(resource):
    assert resource.terms() == ['amendment', 'article', 'date', 'text']
    articles = resource.terms('article')
    articles.remove('Preamble')
    assert sorted(map(int, articles)) == range(1, 8)
    assert sorted(map(int, resource.terms('amendment'))) == range(1, 28)
    assert resource.terms('text/:0') == []
    assert resource.terms('text/right:right~') == resource.terms('text/right*') == ['right', 'rights']
    assert resource.terms('text/writ*') == ['writ', 'writing', 'writings', 'writs', 'written']
    assert resource.terms('text/*?count=0') == []
    assert resource.terms('text/writ*?count=10') == ['writs', 'writ', 'writing', 'writings', 'written']
    assert resource.terms('text/writ*?count=3') == ['writs', 'writ', 'writing']
    assert resource.terms('text/right~1') == ['eight', 'right', 'rights']
    assert resource.terms('text/right~') == ['eight', 'high', 'right', 'rights']
    assert resource.terms('text/right~?count=3') == ['right', 'eight', 'rights']
    assert resource.terms('text/right~?count=5') == ['right', 'eight', 'rights', 'high']
    assert resource.terms('text/write~?count=5') == ['writs', 'writ', 'crime', 'written']
    docs = resource.terms('text/people/docs')
    assert resource.terms('text/people') == len(docs) == 8
    counts = dict(resource.terms('text/people/docs/counts'))
    assert sorted(counts) == docs and all(counts.values()) and sum(counts.values()) > len(counts)
    positions = dict(resource.terms('text/people/docs/positions'))
    assert sorted(positions) == docs and list(map(len, positions.values())) == counts.values()
    assert resource.client.get('terms/text/people/missing').status_code == httplib.NOT_FOUND
    assert resource.client.get('terms/text/people/docs/missing').status_code == httplib.NOT_FOUND


def test_search(resource):
    doc, = resource.search(q='amendment:1', fields='', **{'fields.indexed': 'article,amendment:int'})['docs']
    assert doc['amendment'] == 1 and not doc['article']
    result = resource.search(**{'q.field': 'text', 'q': 'write "hello world"', 'spellcheck': 3})
    terms = result['spellcheck'].pop('text')
    assert result['docs'] == [] and result['spellcheck'] == {}
    assert terms == {'write': ['writs', 'writ', 'crime'], 'world': ['would', 'hold', 'gold'], 'hello': ['held', 'well']}
    result = resource.search(**{'q.field': 'text', 'q': 'write "hello world"', 'q.spellcheck': 'true'})
    assert result['query'] == 'text:writs text:"held would"'
    assert result['count'] == len(result['docs']) == resource.get('/terms/text/writs') == 2
    assert resource.search(q='Preamble', **{'q.field': 'article'})['count'] == 0
    result = resource.search(q='Preamble', **{'q.field': 'article', 'q.type': 'prefix'})
    assert result['count'] == 1 and result['query'] == 'article:Preamble*'
    result = resource.search(q='text:"We the People"', **{'q.phraseSlop': 3})
    assert 0 < result['maxscore'] < 1 and result['count'] == 1
    assert result['query'] == 'text:"we ? people"~3'
    doc, = result['docs']
    assert sorted(doc) == ['__id__', '__score__', 'article']
    assert doc['article'] == 'Preamble' and doc['__id__'] >= 0 and 0 < doc['__score__'] < 1
    result = resource.search(q='text:people')
    docs = result['docs']
    assert sorted(docs, key=operator.itemgetter('__score__'), reverse=True) == docs
    assert len(docs) == result['count'] == 8
    result = resource.search(q='text:people', count=5)
    maxscore = result['maxscore']
    assert docs[:5] == result['docs'] and result['count'] == len(docs)
    result = resource.search(q='text:people', count=5, sort='-amendment:int')
    assert math.isnan(result['maxscore']) and all(math.isnan(doc['__score__']) for doc in result['docs'])
    assert [doc['amendment'] for doc in result['docs']] == ['17', '10', '9', '4', '2']
    result = resource.search(q='text:people', sort='-amendment:int')
    assert [doc.get('amendment') for doc in result['docs']] == ['17', '10', '9', '4', '2', '1', None, None]
    result = resource.search(q='text:people', count=5, sort='-amendment:int', **{'sort.scores': ''})
    assert math.isnan(result['maxscore']) and maxscore in (doc['__score__'] for doc in result['docs'])
    result = resource.search(q='text:people', count=1, sort='-amendment:int', **{'sort.scores': 'max'})
    assert maxscore == result['maxscore'] and maxscore not in (doc['__score__'] for doc in result['docs'])
    result = resource.search(q='text:people', count=5, sort='-article,amendment:int')
    assert [doc.get('amendment') for doc in result['docs']] == [None, None, '1', '2', '4']
    assert [doc['__keys__'] for doc in result['docs']] == [['Preamble', 0], ['1', 0], [None, 1], [None, 2], [None, 4]]
    result = resource.search(q='text:people', start=2, count=2, facets='article,amendment')
    assert [doc['amendment'] for doc in result['docs']] == ['10', '1']
    assert result['count'] == sum(sum(facets.values()) for facets in result['facets'].values())
    for name, keys in [('article', ['1', 'Preamble']), ('amendment', ['1', '10', '17', '2', '4', '9'])]:
        assert sorted(key for key, value in result['facets'][name].items() if value) == keys
    result = resource.search(q='text:president', facets='date')
    assert len(result['facets']['date']) == sum(result['facets']['date'].values()) == 7
    result = resource.search(q='text:freedom')
    assert result['count'] == 1
    doc, = result['docs']
    assert doc['amendment'] == '1'
    assert resource.delete('search', params={'q': 'text:freedom'}) is None
    assert resource.post('update') == 34
    assert resource.search(q='text:freedom')['docs'] == []


def test_highlights(resource):
    doc, = resource.search(q='amendment:1', hl='amendment', fields='article')['docs']
    assert doc['__highlights__'] == {'amendment': ['<strong>1</strong>']}
    doc, = resource.search(q='amendment:1', hl='amendment,article', **{'hl.count': 2, 'hl.tag': 'em'})['docs']
    assert doc['__highlights__'] == {'amendment': ['<em>1</em>']}
    result = resource.search(q='text:1', hl='amendment,article')
    highlights = [doc['__highlights__'] for doc in result['docs']]
    assert all(highlight and not any(highlight.values()) for highlight in highlights)
    result = resource.search(q='text:1', hl='article', **{'hl.enable': 'fields'})
    highlights = [doc['__highlights__'] for doc in result['docs']]
    highlight, = [highlight['article'] for highlight in highlights if highlight.get('article')]
    assert highlight == ['<strong>1</strong>']
    result = resource.search(q='text:"section 1"', hl='amendment,article', **{'hl.enable': 'fields'})
    highlights = [doc['__highlights__'] for doc in result['docs']]
    assert all(highlight and not any(highlight.values()) for highlight in highlights)
    result = resource.search(q='text:"section 1"', hl='amendment,article', **{'hl.enable': ['fields', 'terms']})
    highlights = [doc['__highlights__'] for doc in result['docs']]
    highlight, = [highlight['article'] for highlight in highlights if highlight.get('article')]
    assert highlight == ['<strong>1</strong>']
    result = resource.search(mlt=0)
    assert result['count'] == 25 and set(result['query'].split()) == {'text:united', 'text:states'}
    assert [doc['amendment'] for doc in result['docs'][:4]] == ['10', '11', '15', '19']
    result = resource.search(q='amendment:2', mlt=0, **{'mlt.fields': 'text', 'mlt.minTermFreq': 1, 'mlt.minWordLen': 6})
    assert result['count'] == 11 and set(result['query'].split()) == {'text:necessary', 'text:people'}
    assert [doc['amendment'] for doc in result['docs'][:4]] == ['2', '9', '10', '1']
    result = resource.search(q='text:people', count=1, timeout=-1)
    assert result == {'query': 'text:people', 'count': None, 'maxscore': None, 'docs': []}
    result = resource.search(q='text:people', timeout=0.01)
    assert result['count'] in (None, 8) and (result['maxscore'] is None or result['maxscore'] > 0)
    result = resource.search(q='+text:right +text:people')
    assert result['count'] == 4 and 0 < result['maxscore'] < 1.0
    result = resource.search(q='text:right', group='date', count=2, **{'group.count': 2})
    assert 'docs' not in result and len(result['groups']) == 2
    assert sum(map(operator.itemgetter('count'), result['groups'])) < result['count'] == 13
    assert all(min(group['count'], 2) >= len(group['docs']) for group in result['groups'])
    assert all(doc.get('date') == group['value'] for group in result['groups'] for doc in group['docs'])
    group = result['groups'][0]
    assert group['value'] == '1791-12-15'
    assert sorted(group) == ['count', 'docs', 'value'] and group['count'] == 5
    assert len(group['docs']) == 2 and group['docs'][0]['amendment'] == '2'
    assert len(result['groups'][1]['docs']) == 1 and all(group['docs'] == [] for group in result['groups'][2:])
    result = resource.search(q='text:right', group='amendment:int')
    assert set(map(operator.itemgetter('count'), result['groups'])) == {1}
    assert all(int(doc.get('amendment', 0)) == group['value'] for group in result['groups'] for doc in group['docs'])
    assert result['groups'][0]['value'] == 2 and result['groups'][-1]['value'] == 0
    assert resource.search(q='hello', **{'q.field': 'body.title^2.0'})['query'] == 'body.title:hello^2.0'


def test_facets(tempdir, servers, zipcodes):
    writer = engine.IndexWriter(tempdir)
    writer.commit()
    resource = servers.start(servers.ports[0], '-r', tempdir)
    writer.set('zipcode', engine.NumericField, type=int, stored=True)
    writer.fields['location'] = engine.NestedField('county.city')
    for doc in zipcodes:
        if doc['state'] == 'CA':
            writer.add(zipcode=doc['zipcode'], location='{}.{}'.format(doc['county'], doc['city']))
    writer.commit()
    assert resource.post('update') == resource().popitem()[1] == len(writer)
    terms = resource.terms(urllib.quote('zipcode:int'))
    assert len(terms) == len(writer) and terms[0] == 90001
    terms = resource.terms(urllib.quote('zipcode:int'), step=16)
    assert terms == [65536]
    result = resource.search(count=0, facets='county')
    facets = result['facets']['county']
    assert result['count'] == sum(facets.values()) and 'Los Angeles' in facets
    result = resource.search(q='Los Angeles', count=0, facets='county.city', **{'q.type': 'term', 'q.field': 'county'})
    facets = result['facets']['county.city']
    assert result['count'] == sum(facets.values()) and all(location.startswith('Los Angeles.') for location in facets)
    result = resource.search(count=0, facets='county', **{'facets.count': 3})
    assert sorted(result['facets']['county']) == ['Los Angeles', 'Orange', 'San Diego']
    result = resource.search(count=0, facets='county', **{'facets.min': 140})
    assert sorted(result['facets']['county']) == ['Los Angeles', 'Orange', 'San Diego']
    result = resource.search(q='Los Angeles', group='county.city', **{'group.count': 2, 'q.field': 'county', 'q.type': 'prefix'})
    assert all(group['value'].startswith('Los Angeles') for group in result['groups'])
    assert sum(map(operator.itemgetter('count'), result['groups'])) == sum(facets.values()) == result['count']


def test_queries(servers):
    resource = servers.start(servers.ports[0], **{'tools.validate.etag': False})
    resource.headers['content-length'] = '0'
    assert resource.queries() == []
    resource.client.get('queries/default').status_code == httplib.NOT_FOUND
    resource.client.put('queries/default/alpha', '*:*').status_code == httplib.CREATED
    assert resource.put('queries/default/alpha', 'name:alpha') == 'name:alpha'
    assert resource.put('queries/default/bravo', 'name:bravo') == 'name:bravo'
    assert resource.queries() == ['default']
    assert resource.queries('default') == {'alpha': 0.0, 'bravo': 0.0}
    assert resource.queries('default/alpha') == 'name:alpha'
    resource.client.get('queries/charlie').status_code == httplib.NOT_FOUND
    queries = resource.post('queries/default', {'name': 'alpha'})
    assert queries['alpha'] > 0.0 and queries['bravo'] == 0.0
    queries = resource.post('queries/default', {'name': 'alpha bravo alpha'})
    assert queries['alpha'] > queries['bravo'] > 0.0
    assert resource.delete('queries/default/alpha') == 'name:alpha'
    assert resource.delete('queries/default/alpha') is None
    assert resource.queries('default') == {'bravo': 0.0}


def test_realtime(tempdir, servers):
    for args in [('-r',), ('--real-time', 'index0', 'index1'), ('-r', '--real-time', 'index')]:
        assert subprocess.call((sys.executable, '-m', 'lupyne.server') + args, stderr=subprocess.PIPE)
    root = server.WebIndexer(tempdir)
    root.indexer.add()
    assert root.update() == 1
    del root
    resource = servers.start(servers.ports[0], '--real-time', **{'tools.validate.expires': 0})
    client = resource.client
    response = client.get('docs')
    version, modified, expires = map(response.headers.get, ('etag', 'last-modified', 'expires'))
    assert response.ok and modified is None and response.json() == []
    assert client.post('docs', [{}]).status_code == httplib.OK
    assert resource.docs() == [0]
    assert client.delete('search').status_code == httplib.OK
    assert resource.docs() == []
    time.sleep(max(0, calendar.timegm(parsedate(expires)) + 1 - time.time()))
    assert client.post(json=[tempdir]).status_code == httplib.OK
    response = client.get('docs')
    assert response.ok and response.json() == [0] and expires != response.headers['expires']
    assert client.post('update').ok
    response = client.get('docs')
    assert response.ok and version != response.headers['etag']


def test_start(tempdir, servers):
    port = servers.ports[0]
    pidfile = os.path.join(tempdir, 'pid')
    servers.start(port, '-dp', pidfile)
    time.sleep(1)
    os.kill(int(open(pidfile).read()), signal.SIGTERM)
    del servers[port]
    assert subprocess.call((sys.executable, '-m', 'lupyne.server', '-c', __file__), stderr=subprocess.PIPE)
    assert subprocess.call((sys.executable, 'lupyne/server.py', '-x'), env={'PYTHONPATH': '.'}, stderr=subprocess.PIPE) == 2
    assert cherrypy.tree.mount(None)
    server.init(vmargs=None)
    with pytest.raises(AttributeError):
        server.start(config=True)


def test_config(tempdir, servers):
    engine.IndexWriter(tempdir).close()
    config = {'tools.json_out.indent': 2, 'tools.validate.last_modified': True, 'tools.validate.expires': 0, 'tools.validate.max_age': 0}
    client = servers.start(servers.ports[0], tempdir, tempdir, '--autoreload=0.1', **config).client
    response = client.get()
    assert response.ok
    (directory, size), = response.json().items()
    assert 'Directory@' in directory and size == 0
    assert int(response.headers['age']) >= 0
    assert response.headers['cache-control'] == 'max-age=0'
    assert float(response.headers['x-response-time']) > 0.0
    dates = [parsedate(response.headers[header]) for header in ('last-modified', 'expires', 'date')]
    assert all(dates) and sorted(dates) == dates
    w, verion, _ = response.headers['etag'].split('"')
    assert w == 'W/' and verion.isdigit()


def test_example(request, servers):
    """Custom server example (only run explicitly)."""
    if request.config.option.verbose < 0:
        pytest.skip("requires verbose output")
    servers.module = 'examples.server'
    resource = servers.start(servers.ports[0])
    result = resource.search(q='date:17*', group='date')
    assert {group['value']: group['count'] for group in result['groups']} == {'1795-02-07': 1, '1791-12-15': 10}
    result = resource.search(count=0, facets='year')
    facets = result['facets']['year']
    assert not result['docs'] and facets['1791'] == 10 and sum(facets.values()) == result['count']
    result = resource.search(q='text:right', facets='year')
    facets = result['facets']['year']
    assert len(result['docs']) == result['count'] == sum(facets.values())


def test_replication(tempdir, servers):
    directory = os.path.join(tempdir, 'backup')
    primary = servers.start(servers.ports[0], tempdir)
    sync, update = '--autosync=' + primary.url, '--autoupdate=1'
    secondary = servers.start(servers.ports[1], '-r', directory, sync, update)
    resource = servers.start(servers.ports[2], '-r', directory)
    for args in [('-r', tempdir), (update, tempdir), (update, tempdir, tempdir)]:
        assert subprocess.call((sys.executable, '-m', 'lupyne.server', sync) + args, stderr=subprocess.PIPE)
    primary.post('docs', [{}])
    assert primary.post('update') == 1
    assert primary.client.put('update/0').status_code == httplib.NOT_FOUND
    response = resource.client.post(json={'url': primary.url})
    assert response.status_code == httplib.ACCEPTED and sum(response.json().values()) == 0
    assert resource.post('update') == 1
    assert resource.post(json={'url': primary.url})
    assert resource.post('update') == 1
    primary.post('docs', [{}])
    assert primary.post('update') == 2
    time.sleep(1.1)
    assert sum(secondary().values()) == 2
    servers.stop(servers.ports[-1])
    root = server.WebSearcher(directory, urls=(primary.url, secondary.url))
    app = server.mount(root)
    root.fields = {}
    assert root.update() == 2
    assert len(root.urls) == 2
    servers.stop(servers.ports[0])
    assert secondary.docs()
    assert secondary.client.post('docs', []).status_code == httplib.METHOD_NOT_ALLOWED
    assert secondary.terms(option='indexed') == []
    assert root.update() == 2
    assert len(root.urls) == 1
    servers.stop(servers.ports[1])
    assert root.update() == 2
    assert len(root.urls) == 0 and isinstance(app.root, server.WebIndexer)
    app.root.close()
    root = server.WebSearcher(directory)
    app = server.mount(root, autoupdate=0.1)
    root.fields, root.autoupdate = {}, 0.1
    cherrypy.config['log.screen'] = servers.config['log.screen']
    cherrypy.engine.state = cherrypy.engine.states.STARTED
    root.monitor.start()  # simulate engine starting
    time.sleep(0.2)
    app.root.indexer.add()
    time.sleep(0.2)
    assert len(app.root.indexer) == len(root.searcher) + 1
    app.root.monitor.unsubscribe()
    del app.root
