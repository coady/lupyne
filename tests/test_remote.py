from future_builtins import map
import os
import sys
import tempfile
import shutil
import signal
import subprocess
import operator
import httplib
import math
import json
import time
import calendar
import errno
import contextlib
from email.utils import parsedate
import pytest
import lucene
import cherrypy
from lupyne import client, engine, server
from .fixtures import warns, tempdir, constitution, zipcodes


class Servers(dict):
    module = 'lupyne.server'
    ports = 8080, 8081, 8082
    hosts = list(map('localhost:{:d}'.format, ports))
    config = {'server.socket_timeout': 2, 'server.shutdown_timeout': 1}

    def start(self, port, *args, **config):
        "Start server in separate process on given port."
        config.update(self.config)
        config['server.socket_port'] = port
        cherrypy.process.servers.wait_for_free_port('localhost', port)
        server = self[port] = subprocess.Popen((sys.executable, '-m', self.module, '-c', json.dumps(config)) + args)
        cherrypy.process.servers.wait_for_occupied_port('localhost', port)
        server.started = time.time()
        assert not server.poll()

    def stop(self, port):
        "Terminate server on given port."
        server = self.pop(port)
        time.sleep(max(0, server.started + 0.1 - time.time()))
        server.terminate()
        assert server.wait() == 0


@pytest.yield_fixture
def servers(request):
    servers = Servers()
    servers.config['log.screen'] = request.config.option.verbose > 0
    yield servers
    for port in list(servers):
        servers.stop(port)


@contextlib.contextmanager
def raises(exception, code):
    "Assert an exception is raised with specific code."
    try:
        yield
    except exception as exc:
        assert exc[0] == code
    else:
        raise AssertionError(exception.__name__ + ' not raised')


class Resource(client.Resource):
    "Modify status and inject headers to test warning framework."
    def getresponse(self):
        response = client.Resource.getresponse(self)
        response.msg.addheader('warning', '199 lupyne "test warning"')
        if response.status == httplib.NOT_FOUND:
            response.status = httplib.MOVED_PERMANENTLY
            response.msg.addheader('location', 'http://{}:{}'.format(self.host, self.port))
        return response


def test_interface(tempdir, servers):
    "Remote reading and writing."
    config = {'tools.json_out.indent': 2, 'tools.validate.last_modified': True, 'tools.validate.expires': 0, 'tools.validate.max_age': 0}
    servers.start(servers.ports[0], tempdir, '--autoreload=1', **config)
    servers.start(servers.ports[1], tempdir, tempdir, '--autoupdate=2.0')  # concurrent searchers
    resource = client.Resource('localhost', servers.ports[0])
    assert resource.get('/favicon.ico')
    response = resource.call('GET', '/')
    assert response.status == httplib.OK and response.reason == 'OK' and response.time > 0 and '\n' in response.body
    assert response.getheader('content-encoding') == 'gzip' and response.getheader('content-type').startswith('application/json')
    version, modified = response.getheader('etag'), response.getheader('last-modified')
    assert version.strip('W/"').isdigit()
    assert int(response.getheader('age')) >= 0 and response.getheader('cache-control') == 'max-age=0'
    dates = list(map(parsedate, [modified, response.getheader('expires'), response.getheader('date')]))
    assert all(dates) and sorted(dates) == dates
    (directory, count), = response().items()
    assert count == 0 and 'Directory@' in directory
    assert resource.call('HEAD', '/').status == httplib.OK
    with raises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
        resource.post('/terms')
    with raises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
        resource.get('/update')
    with raises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
        resource.post('/update/snapshot')
    with raises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
        resource.put('/fields')
    with raises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
        resource.post('/docs/x/y')
    with raises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
        resource.put('/docs/0')
    with raises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
        resource.delete('/docs')
    httplib.HTTPConnection.request(resource, 'POST', '/docs', headers={'content-length': '0', 'content-type': 'application/json'})
    assert resource.getresponse().status == httplib.BAD_REQUEST
    httplib.HTTPConnection.request(resource, 'POST', '/docs', headers={'content-length': '0', 'content-type': 'application/x-www-form-urlencoded'})
    assert resource.getresponse().status == httplib.UNSUPPORTED_MEDIA_TYPE
    httplib.HTTPConnection.request(resource, 'GET', '/', headers={'accept': 'text/html'})
    assert resource.getresponse().status == httplib.NOT_ACCEPTABLE
    assert resource.get('/docs') == []
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/docs/0')
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/docs/x/y')
    try:
        assert resource.get('/docs/~')
    except httplib.HTTPException as exc:
        status, reason, body = exc
        assert body['status'] == '404 Not Found'
        assert body['message'].startswith('invalid literal for int')
        assert body['message'] in body['traceback']
    assert resource.get('/fields') == []
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/fields/name')
    assert resource.get('/terms') == []
    assert resource.get('/terms/x') == []
    assert resource.get('/terms/x/:') == []
    assert resource.get('/terms/x/y') == 0
    assert resource.get('/terms/x/y/docs') == []
    assert resource.get('/terms/x/y/docs/counts') == []
    assert resource.get('/terms/x/y/docs/positions') == []
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/terms/x/y/missing')
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/terms/x/y/docs/missing')
    defaults = {'indexed': True}
    response = resource.call('PUT', '/fields/text')
    assert response.getheader('etag') is None
    assert response.status == httplib.CREATED and response() == defaults
    response = resource.call('PUT', '/fields/text', {})
    assert response.status == httplib.OK and response() == defaults
    assert resource.put('/fields/name', {'stored': True, 'tokenized': False})
    assert sorted(resource.get('/fields')) == ['name', 'text']
    assert resource.get('/fields/text') == {'indexed': True}
    assert not resource.post('/docs', [{'name': 'sample', 'text': 'hello world'}])
    assert not resource.post('/docs')
    (directory, count), = resource.get('/').items()
    assert count == 1
    assert resource.get('/docs') == []
    result = resource.get('/search?q=text:hello')
    assert math.isnan(result.pop('maxscore'))
    assert result == {'query': 'text:hello', 'count': 0, 'docs': []}
    resource.headers['if-none-match'] = version
    response = resource.call('GET', '/', redirect=True)
    assert response.status == httplib.NOT_MODIFIED and response.getheader('etag') == version
    del resource.headers['if-none-match']
    resource.headers['if-modified-since'] = modified
    assert resource.call('GET', '/').status == httplib.NOT_MODIFIED
    del resource.headers['if-modified-since']
    time.sleep(max(0, calendar.timegm(parsedate(modified)) + 1 - time.time()))
    assert resource.post('/update')
    response = resource.call('GET', '/')
    assert response and response.getheader('etag') != version and parsedate(response.getheader('last-modified')) > parsedate(modified)
    resource.headers['if-match'] = version
    response = resource.call('GET', '/docs/0')
    assert response.status == httplib.PRECONDITION_FAILED and '\n' in response.body
    del resource.headers['if-match']
    assert resource.get('/docs') == [0]
    assert resource.get('/docs/0') == resource.get('/docs/name/sample') == {'name': 'sample'}
    assert resource.get('/docs/0', fields='missing') == {'missing': None}
    assert resource.get('/docs/0', fields='', **{'fields.multi': 'missing'}) == {'missing': []}
    assert resource.get('/terms') == resource.get('/terms', indexed='true') == ['name', 'text']
    assert resource.get('/terms', indexed='false') == []
    assert resource.get('/terms/text') == ['hello', 'world']
    assert resource.get('/terms/text/world') == 1
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        resource.get('/terms/text/world~-')
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        resource.get('/terms/text/world~?count=')
    assert resource.get('/terms/text/world?count=1')
    assert resource.get('/terms/text/world/docs') == [0]
    assert resource.get('/terms/text/world/docs/counts') == [[0, 1]]
    assert resource.get('/terms/text/world/docs/positions') == [[0, [1]]]
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        resource.get('/search?count=')
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        resource.get('/search', count=1, sort='x:str')
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        resource.get('/search', count=1, group='x:str')
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        resource.get('/search', q='')
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        resource.get('/search?q.test=True')
    assert resource.get('/search', count=0) == {'count': 1, 'maxscore': 1.0, 'query': None, 'docs': []}
    assert resource.get('/search', fields='')['docs'] == [{'__id__': 0, '__score__': 1.0}]
    hit, = resource.get('/search', fields='', **{'fields.multi': 'name'})['docs']
    assert hit == {'__id__': 0, 'name': ['sample'], '__score__': 1.0}
    hit, = resource.get('/search', q='name:sample', fields='', hl='name')['docs']
    assert sorted(hit) == ['__highlights__', '__id__', '__score__']
    result = resource.get('/search', q='text:hello')
    assert result == resource.get('/search?q=hello&q.field=text')
    assert result['count'] == resource.get('/search')['count'] == 1
    assert result['query'] == 'text:hello'
    assert 0 < result['maxscore'] < 1
    doc, = result['docs']
    assert sorted(doc) == ['__id__', '__score__', 'name']
    assert doc['__id__'] == 0 and doc['__score__'] > 0 and doc['name'] == 'sample'
    hit, = resource.get('/search', q='hello world', **{'q.field': ['text', 'body']})['docs']
    assert hit['__id__'] == doc['__id__'] and hit['__score__'] < doc['__score__']
    hit, = resource.get('/search', q='hello world', **{'q.field': 'text', 'q.op': 'and'})['docs']
    assert hit['__id__'] == doc['__id__'] and hit['__score__'] > doc['__score__']
    result = resource.get('/search?q=hello+world&q.field=text^4&q.field=body')
    assert result['query'] == '(body:hello text:hello^4.0) (body:world text:world^4.0)'
    assert resource.get('/search', q='hello', **{'q.field': 'body.title^2.0'})['query'] == 'body.title:hello^2.0'
    hit, = result['docs']
    assert hit['__id__'] == doc['__id__'] and hit['__score__'] > doc['__score__']
    result = resource.get('/search', facets='name', spellcheck=1)
    assert result['facets'] == {'name': {'sample': 1}} and result['spellcheck'] == {}
    resource = client.Resource('localhost', servers.ports[1])
    assert set(resource.get('/').values()) < {0, 1}
    assert resource.post('/update', {'filters': True, 'sorters': True}) == 2
    assert resource.get('/docs') == [0, 1]
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/docs/name/sample')
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/fields')
    resource = client.Resource('localhost', servers.ports[0])
    assert not resource.delete('/search', q='sample', **{'q.field': 'name', 'q.type': 'term'})
    assert resource.get('/docs') == [0]
    assert not resource.post('/update', {'merge': True})
    assert resource.get('/docs') == []
    assert not resource.put('/docs/name/sample')
    assert resource.post('/update')
    assert resource.get('/docs/name/sample')
    assert not resource.put('/docs/name/sample')
    assert resource.post('/update')
    assert resource.get('/docs/name/sample')
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        assert resource.put('/fields/name', {'omit': True})
    with raises(httplib.HTTPException, httplib.BAD_REQUEST):
        resource.put('/docs/name/sample', {'name': 'mismatched'})
    with raises(httplib.HTTPException, httplib.CONFLICT):
        resource.put('/docs/missing/sample')
    assert resource.put('/fields/name', {'stored': True, 'indexed': False, 'omitNorms': True})
    with raises(httplib.HTTPException, httplib.CONFLICT):
        resource.put('/docs/name/sample')
    assert not resource.delete('/docs/missing/sample')
    resource.post('/update')
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/docs/missing/sample')
    assert not resource.delete('/search')
    responses = list(resource.multicall(('POST', '/docs', [{}]), ('POST', '/update'), ('GET', '/docs')))
    assert responses[0].status == httplib.ACCEPTED and responses[1]() == len(responses[2]()) == 1
    tmpdir = tempfile.mkdtemp(dir=os.path.dirname(__file__))
    engine.IndexWriter(tmpdir).add()
    assert resource.post('/', [tmpdir]).values() == [2]
    shutil.rmtree(tmpdir)
    with warns(DeprecationWarning, UserWarning):
        assert Resource(resource.host, resource.port).call('GET', '/missing', redirect=True)()
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/update/snapshot')
    response = resource.call('PUT', '/update/snapshot')
    assert response.status == httplib.CREATED
    path = response.getheader('location')
    names = response()
    assert all(name.startswith('_') or name.startswith('segments_') for name in names)
    assert resource.get(path) == names
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get(path + '/segments')
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.download(path + '/segments.gen', tempdir)
    for name in names:
        response = resource.call('GET', path + '/' + name)
        assert response and response.getheader('content-type') == 'application/x-download'
        assert len(response.body) == os.path.getsize(os.path.join(tempdir, name))
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        assert resource.put('/update/backup') == names
    resource = client.Resource('localhost', servers.ports[1] + 1)
    with raises(IOError, errno.ECONNREFUSED):
        resource.get('/')
    port = servers.ports[0]
    servers.stop(port)
    pidfile = os.path.join(tempdir, 'pid')
    servers.start(port, '-dp', pidfile)
    time.sleep(1)
    os.kill(int(open(pidfile).read()), signal.SIGTERM)
    del servers[port]
    assert subprocess.call((sys.executable, '-m', 'lupyne.server', '-c', __file__), stderr=subprocess.PIPE)
    assert cherrypy.tree.mount(None)
    server.init(vmargs=None)
    with pytest.raises(AttributeError):
        server.start(config=True)


def test_basic(tempdir, servers, constitution):
    "Remote text indexing and searching."
    servers.start(servers.ports[0], tempdir)
    resource = client.Resource('localhost', servers.ports[0])
    assert resource.get('/fields') == []
    for name, settings in constitution.fields.items():
        assert resource.put('/fields/' + name, settings)
    fields = resource.get('/fields')
    assert sorted(fields) == ['amendment', 'article', 'date', 'text']
    for field in fields:
        assert resource.get('/fields/' + name)['indexed']
    resource.post('/docs', list(constitution))
    assert resource.get('/').values() == [35]
    resource.post('/update', {'spellcheckers': True, 'merge': 1})
    assert resource.get('/docs/0', **{'fields.indexed': 'amendment:int'}) == {'amendment': 0, 'article': 'Preamble'}
    doc = resource.get('/docs/0', **{'fields.vector': 'text,missing'})
    assert doc['missing'] == [] and doc['text'].index('states') < doc['text'].index('united')
    doc = resource.get('/docs/0', **{'fields.vector.counts': 'text'})
    with raises(httplib.HTTPException, httplib.CONFLICT):
        resource.patch('/docs/amendment/1', {'amendment': '1'})
    assert not resource.patch('/docs/amendment/1')
    assert sorted(term for term, count in doc['text'].items() if count > 1) == ['establish', 'states', 'united']
    assert resource.get('/terms') == ['amendment', 'article', 'date', 'text']
    articles = resource.get('/terms/article')
    articles.remove('Preamble')
    assert sorted(map(int, articles)) == range(1, 8)
    assert sorted(map(int, resource.get('/terms/amendment'))) == range(1, 28)
    assert resource.get('/terms/text/:0') == []
    assert resource.get('/terms/text/right:right~') == resource.get('/terms/text/right*') == ['right', 'rights']
    assert resource.get('/terms/text/writ*') == ['writ', 'writing', 'writings', 'writs', 'written']
    assert resource.get('/terms/text/*?count=0') == []
    assert resource.get('/terms/text/writ*?count=10') == ['writs', 'writ', 'writing', 'writings', 'written']
    assert resource.get('/terms/text/writ*?count=3') == ['writs', 'writ', 'writing']
    assert resource.get('/terms/text/right~1') == ['eight', 'right', 'rights']
    assert resource.get('/terms/text/right~') == ['eight', 'high', 'right', 'rights']
    assert resource.get('/terms/text/right~?count=3') == ['right', 'eight', 'rights']
    assert resource.get('/terms/text/right~?count=5') == ['right', 'eight', 'rights', 'high']
    assert resource.get('/terms/text/write~?count=5') == ['writs', 'writ', 'crime', 'written']
    docs = resource.get('/terms/text/people/docs')
    assert resource.get('/terms/text/people') == len(docs) == 8
    counts = dict(resource.get('/terms/text/people/docs/counts'))
    assert sorted(counts) == docs and all(counts.values()) and sum(counts.values()) > len(counts)
    positions = dict(resource.get('/terms/text/people/docs/positions'))
    assert sorted(positions) == docs and list(map(len, positions.values())) == counts.values()
    doc = resource.get('/docs/amendment/1', **{'fields.multi': 'text', 'fields.indexed': 'text'})
    assert doc['text'][:3] == ['abridging', 'assemble', 'congress']
    doc, = resource.get('/search', q='amendment:1', fields='', **{'fields.indexed': 'article,amendment:int'})['docs']
    assert doc['amendment'] == 1 and not doc['article']
    result = resource.get('/search', **{'q.field': 'text', 'q': 'write "hello world"', 'spellcheck': 3})
    terms = result['spellcheck'].pop('text')
    assert result['docs'] == [] and result['spellcheck'] == {}
    assert terms == {'write': ['writs', 'writ', 'crime'], 'world': ['would', 'hold', 'gold'], 'hello': ['held', 'well']}
    result = resource.get('/search', **{'q.field': 'text', 'q': 'write "hello world"', 'q.spellcheck': 'true'})
    assert result['query'] == 'text:writs text:"held would"'
    assert result['count'] == len(result['docs']) == resource.get('/terms/text/writs') == 2
    assert resource.get('/search', q='Preamble', **{'q.field': 'article'})['count'] == 0
    result = resource.get('/search', q='Preamble', **{'q.field': 'article', 'q.type': 'prefix'})
    assert result['count'] == 1 and result['query'] == 'article:Preamble*'
    result = resource.get('/search', q='text:"We the People"', **{'q.phraseSlop': 3})
    assert 0 < result['maxscore'] < 1 and result['count'] == 1
    assert result['query'] == 'text:"we ? people"~3'
    doc, = result['docs']
    assert sorted(doc) == ['__id__', '__score__', 'article']
    assert doc['article'] == 'Preamble' and doc['__id__'] >= 0 and 0 < doc['__score__'] < 1
    result = resource.get('/search', q='text:people')
    docs = result['docs']
    assert sorted(docs, key=operator.itemgetter('__score__'), reverse=True) == docs
    assert len(docs) == result['count'] == 8
    result = resource.get('/search', q='text:people', count=5)
    maxscore = result['maxscore']
    assert docs[:5] == result['docs'] and result['count'] == len(docs)
    result = resource.get('/search', q='text:people', count=5, sort='-amendment:int')
    assert math.isnan(result['maxscore']) and all(math.isnan(doc['__score__']) for doc in result['docs'])
    assert [doc['amendment'] for doc in result['docs']] == ['17', '10', '9', '4', '2']
    result = resource.get('/search', q='text:people', sort='-amendment:int')
    assert [doc.get('amendment') for doc in result['docs']] == ['17', '10', '9', '4', '2', '1', None, None]
    result = resource.get('/search', q='text:people', count=5, sort='-amendment:int', **{'sort.scores': ''})
    assert math.isnan(result['maxscore']) and maxscore in (doc['__score__'] for doc in result['docs'])
    result = resource.get('/search', q='text:people', count=1, sort='-amendment:int', **{'sort.scores': 'max'})
    assert maxscore == result['maxscore'] and maxscore not in (doc['__score__'] for doc in result['docs'])
    result = resource.get('/search', q='text:people', count=5, sort='-article,amendment:int')
    assert [doc.get('amendment') for doc in result['docs']] == [None, None, '1', '2', '4']
    assert [doc['__keys__'] for doc in result['docs']] == [['Preamble', 0], ['1', 0], [None, 1], [None, 2], [None, 4]]
    result = resource.get('/search', q='text:people', start=2, count=2, facets='article,amendment')
    assert [doc['amendment'] for doc in result['docs']] == ['10', '1']
    assert result['count'] == sum(sum(facets.values()) for facets in result['facets'].values())
    for name, keys in [('article', ['1', 'Preamble']), ('amendment', ['1', '10', '17', '2', '4', '9'])]:
        assert sorted(key for key, value in result['facets'][name].items() if value) == keys
    result = resource.get('/search', q='text:president', facets='date')
    assert len(result['facets']['date']) == sum(result['facets']['date'].values()) == 7
    result = resource.get('/search', q='text:freedom')
    assert result['count'] == 1
    doc, = result['docs']
    assert doc['amendment'] == '1'
    doc, = resource.get('/search', q='amendment:1', hl='amendment', fields='article')['docs']
    assert doc['__highlights__'] == {'amendment': ['<strong>1</strong>']}
    doc, = resource.get('/search', q='amendment:1', hl='amendment,article', **{'hl.count': 2, 'hl.tag': 'em'})['docs']
    assert doc['__highlights__'] == {'amendment': ['<em>1</em>']}
    result = resource.get('/search', q='text:1', hl='amendment,article')
    highlights = [doc['__highlights__'] for doc in result['docs']]
    assert all(highlight and not any(highlight.values()) for highlight in highlights)
    result = resource.get('/search', q='text:1', hl='article', **{'hl.enable': 'fields'})
    highlights = [doc['__highlights__'] for doc in result['docs']]
    highlight, = [highlight['article'] for highlight in highlights if highlight.get('article')]
    assert highlight == ['<strong>1</strong>']
    result = resource.get('/search', q='text:"section 1"', hl='amendment,article', **{'hl.enable': 'fields'})
    highlights = [doc['__highlights__'] for doc in result['docs']]
    assert all(highlight and not any(highlight.values()) for highlight in highlights)
    result = resource.get('/search', q='text:"section 1"', hl='amendment,article', **{'hl.enable': ['fields', 'terms']})
    highlights = [doc['__highlights__'] for doc in result['docs']]
    highlight, = [highlight['article'] for highlight in highlights if highlight.get('article')]
    assert highlight == ['<strong>1</strong>']
    result = resource.get('/search', mlt=0)
    assert result['count'] == 25 and set(result['query'].split()) == {'text:united', 'text:states'}
    assert [doc['amendment'] for doc in result['docs'][:4]] == ['10', '11', '15', '19']
    result = resource.get('/search', q='amendment:2', mlt=0, **{'mlt.fields': 'text', 'mlt.minTermFreq': 1, 'mlt.minWordLen': 6})
    assert result['count'] == 11 and set(result['query'].split()) == {'text:necessary', 'text:people'}
    assert [doc['amendment'] for doc in result['docs'][:4]] == ['2', '9', '10', '1']
    result = resource.get('/search', q='text:people', count=1, timeout=-1)
    assert result == {'query': 'text:people', 'count': None, 'maxscore': None, 'docs': []}
    result = resource.get('/search', q='text:people', timeout=0.01)
    assert result['count'] in (None, 8) and result['maxscore'] in (None, maxscore)
    result = resource.get('/search', filter='text:people')
    assert result['count'] == 8 and {doc['__score__'] for doc in result['docs']} == {1.0}
    result = resource.get('/search', q='text:right', filter='text:people')
    assert result['count'] == 4 and 0 < result['maxscore'] < 1.0
    result = resource.get('/search', q='text:right', group='date', count=2, **{'group.count': 2})
    assert 'docs' not in result and len(result['groups']) == 2
    assert sum(map(operator.itemgetter('count'), result['groups'])) < result['count'] == 13
    assert all(min(group['count'], 2) >= len(group['docs']) for group in result['groups'])
    assert all(doc.get('date') == group['value'] for group in result['groups'] for doc in group['docs'])
    group = result['groups'][0]
    assert group['value'] == '1791-12-15'
    assert sorted(group) == ['count', 'docs', 'value'] and group['count'] == 5
    assert len(group['docs']) == 2 and group['docs'][0]['amendment'] == '2'
    assert len(result['groups'][1]['docs']) == 1 and all(group['docs'] == [] for group in result['groups'][2:])
    result = resource.get('/search', q='text:right', group='amendment:int')
    assert set(map(operator.itemgetter('count'), result['groups'])) == {1}
    assert all(int(doc.get('amendment', 0)) == group['value'] for group in result['groups'] for doc in group['docs'])
    assert result['groups'][0]['value'] == 2 and result['groups'][-1]['value'] == 0


def test_advanced(tempdir, servers, zipcodes):
    "Nested and numeric fields."
    writer = engine.IndexWriter(tempdir)
    writer.commit()
    servers.start(servers.ports[0], '-r', tempdir, **{'tools.validate.etag': False})
    writer.set('zipcode', engine.NumericField, type=int, stored=True)
    writer.fields['location'] = engine.NestedField('county.city')
    for doc in zipcodes:
        if doc['state'] == 'CA':
            writer.add(zipcode=doc['zipcode'], location='{}.{}'.format(doc['county'], doc['city']))
    writer.commit()
    resource = client.Resource('localhost', servers.ports[0])
    assert resource.post('/update') == resource.get('/').popitem()[1] == len(writer)
    terms = resource.get('/terms/zipcode:int')
    assert len(terms) == len(writer) and terms[0] == 90001
    terms = resource.get('/terms/zipcode:int?step=16')
    assert terms == [65536]
    result = resource.get('/search', count=0, facets='county')
    facets = result['facets']['county']
    assert result['count'] == sum(facets.values()) and 'Los Angeles' in facets
    result = resource.get('/search', q='Los Angeles', count=0, facets='county.city', **{'q.type': 'term', 'q.field': 'county'})
    facets = result['facets']['county.city']
    assert result['count'] == sum(facets.values()) and all(location.startswith('Los Angeles.') for location in facets)
    result = resource.get('/search', count=0, facets='county', **{'facets.count': 3})
    assert sorted(result['facets']['county']) == ['Los Angeles', 'Orange', 'San Diego']
    result = resource.get('/search', count=0, facets='county', **{'facets.min': 140})
    assert sorted(result['facets']['county']) == ['Los Angeles', 'Orange', 'San Diego']
    result = resource.get('/search', q='Los Angeles', group='county.city', **{'group.count': 2, 'q.field': 'county', 'q.type': 'prefix'})
    assert all(group['value'].startswith('Los Angeles') for group in result['groups'])
    assert sum(map(operator.itemgetter('count'), result['groups'])) == sum(facets.values()) == result['count']
    assert resource.get('/queries') == []
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/queries/default')
    resource.call('PUT', '/queries/default/alpha', '*:*').status == httplib.CREATED
    assert resource.put('/queries/default/alpha', 'name:alpha') == 'name:alpha'
    assert resource.put('/queries/default/bravo', 'name:bravo') == 'name:bravo'
    assert resource.get('/queries') == ['default']
    assert resource.get('/queries/default') == {'alpha': 0.0, 'bravo': 0.0}
    with raises(httplib.HTTPException, httplib.NOT_FOUND):
        resource.get('/queries/default/charlie')
    queries = resource.post('/queries/default', {'name': 'alpha'})
    assert queries['alpha'] > 0.0 and queries['bravo'] == 0.0
    queries = resource.call('GET', '/queries/default', {'name': 'alpha bravo alpha'})()
    assert queries['alpha'] > queries['bravo'] > 0.0
    assert resource.delete('/queries/default/alpha') == 'name:alpha'
    assert resource.delete('/queries/default/alpha') is None
    assert resource.get('/queries/default') == {'bravo': 0.0}


def test_realtime(tempdir, servers):
    "Real Time updating and searching."
    for args in [('-r',), ('--real-time', 'index0', 'index1'), ('-r', '--real-time', 'index')]:
        assert subprocess.call((sys.executable, '-m', 'lupyne.server') + args, stderr=subprocess.PIPE)
    root = server.WebIndexer(tempdir)
    root.indexer.add()
    assert root.update() == 1
    del root
    port = servers.ports[0]
    servers.start(port, '--real-time', **{'tools.validate.expires': 0})
    resource = client.Resource('localhost', port)
    response = resource.call('GET', '/docs')
    version, modified, expires = map(response.getheader, ('etag', 'last-modified', 'expires'))
    assert modified is None and response() == []
    assert resource.call('POST', '/docs', [{}]).status == httplib.OK
    assert resource.get('/docs') == [0]
    assert resource.call('DELETE', '/search').status == httplib.OK
    assert resource.get('/docs') == []
    time.sleep(max(0, calendar.timegm(parsedate(expires)) + 1 - time.time()))
    assert resource.call('POST', '/', [tempdir]).status == httplib.OK
    response = resource.call('GET', '/docs')
    assert response() == [0] and expires != response.getheader('expires')
    resource.post('/update')
    response = resource.call('GET', '/docs')
    assert response and version != response.getheader('etag')


def test_example(request, servers):
    "Custom server example (only run explicitly)."
    if request.config.option.verbose < 0:
        pytest.skip("requires verbose output")
    servers.module = 'examples.server'
    port = servers.ports[0]
    servers.start(port)
    resource = client.Resource('localhost', port)
    result = resource.get('/search', q='date:17*', group='year')
    assert dict(map(operator.itemgetter('value', 'count'), result['groups'])) == {1795: 1, 1791: 10}
    result = resource.get('/search', q='date:17*', group='year', sort='-year')
    assert list(map(operator.itemgetter('value'), result['groups'])) == [1795, 1791]
    result = resource.get('/search', count=0, facets='year')
    facets = result['facets']['year']
    assert not result['docs'] and facets['1791'] == 10 and sum(facets.values()) == result['count']
    result = resource.get('/search', q='text:right', facets='year')
    facets = result['facets']['year']
    assert len(result['docs']) == result['count'] == sum(facets.values())
