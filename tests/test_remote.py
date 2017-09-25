from future_builtins import map
import os
import sys
import tempfile
import shutil
import subprocess
import httplib
import math
import json
import time
import calendar
import errno
import contextlib
from email.utils import parsedate
import pytest
import portend
from lupyne import client, engine


class Servers(dict):
    module = 'lupyne.server'
    ports = 8080, 8081, 8082
    hosts = list(map('localhost:{:d}'.format, ports))
    urls = tuple(map('http://localhost:{}'.format, ports))
    config = {'server.socket_timeout': 2, 'server.shutdown_timeout': 1}

    def start(self, port, *args, **config):
        "Start server in separate process on given port."
        config.update(self.config)
        config['server.socket_port'] = port
        portend.free('localhost', port)
        server = self[port] = subprocess.Popen((sys.executable, '-m', self.module, '-c', json.dumps(config)) + args)
        portend.occupied('localhost', port)
        server.started = time.time()
        assert not server.poll()

    def stop(self, port):
        "Terminate server on given port."
        server = self.pop(port)
        time.sleep(max(0, server.started + 0.1 - time.time()))
        server.terminate()
        assert server.wait() == 0


@pytest.fixture
def servers(request):
    servers = Servers()
    servers.config['log.screen'] = request.config.option.verbose > 0
    yield servers
    for port in list(servers):
        servers.stop(port)


@contextlib.contextmanager
def raises(exception, code):
    "Assert an exception is raised with specific code."
    with pytest.raises(exception) as context:
        yield
    assert context.value[0] == code
    del context  # release exception before exiting block


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
    response = resource.call('GET', '/')
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
    tmpdir = tempfile.mkdtemp(dir=os.path.dirname(__file__))
    engine.IndexWriter(tmpdir).add()
    assert resource.post('/', [tmpdir]).values() == [1]
    shutil.rmtree(tmpdir)
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
