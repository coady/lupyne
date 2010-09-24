from future_builtins import map
import unittest
import os, sys
import subprocess
import operator
import httplib
import math
import json
import socket, errno
from contextlib import contextmanager
import lucene
import cherrypy
from lupyne import client, server
import fixture, local

@contextmanager
def assertRaises(exception, code):
    "Assert an exception is raised with specific code."
    try:
        yield
    except exception as exc:
        assert exc[0] == code, exc
    else:
        raise AssertionError(exception.__name__ + ' not raised')

class BaseTest(local.BaseTest):
    ports = 8080, 8081
    def setUp(self):
        local.BaseTest.setUp(self)
        pidfile = os.path.join(self.tempdir, 'pid')
        self.servers = (
            self.start(self.ports[0], self.tempdir, '--autoreload=1'),
            self.start(self.ports[1], self.tempdir, self.tempdir, '-p', pidfile), # concurrent searchers
        )
        assert int(open(pidfile).read()) == self.servers[-1].pid
    def run(self, result):
        self.verbose = result.showAll
        local.BaseTest.run(self, result)
    def tearDown(self):
        for server in self.servers:
            self.stop(server)
        local.BaseTest.tearDown(self)
    def start(self, port, *args):
        "Start server in separate process on given port."
        config = json.dumps({'server.socket_port': port, 'log.screen': self.verbose})
        cherrypy.process.servers.wait_for_free_port('localhost', port)
        server = subprocess.Popen((sys.executable, '-m', 'lupyne.server', '-c', config) + args)
        cherrypy.process.servers.wait_for_occupied_port('localhost', port)
        assert server.poll() is None
        return server
    def stop(self, server):
        "Terminate server."
        server.terminate()
        assert server.wait() == 0

class TestCase(BaseTest):
    
    def testInterface(self):
        "Remote reading and writing."
        resource = client.Resource('localhost', self.ports[0])
        assert resource.get('/favicon.ico')
        resource.request('GET', '/')
        response = resource.getresponse()
        assert response.status == httplib.OK and response.reason == 'OK'
        assert response.getheader('content-encoding') == 'gzip' and response.getheader('content-type').startswith('text/x-json')
        (directory, count), = response().items()
        assert count == 0 and 'FSDirectory@' in directory
        assert not resource('HEAD', '/')
        with assertRaises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
            resource.put('/')
        with assertRaises(httplib.HTTPException, httplib.METHOD_NOT_ALLOWED):
            resource.post('/fields')
        assert resource.get('/docs') == []
        with assertRaises(httplib.HTTPException, httplib.NOT_FOUND):
            resource.get('/docs/0')
        try:
            assert resource.get('/docs/~')
        except httplib.HTTPException as exc:
            status, reason, body = exc
            assert body['status'] == '404 Not Found'
            assert body['message'].startswith('invalid literal for int')
            assert body['message'] in body['traceback']
        assert resource.get('/fields') == []
        with assertRaises(httplib.HTTPException, httplib.NOT_FOUND):
            resource.get('/fields/name')
        assert resource.get('/terms') == []
        assert resource.get('/terms/x') == []
        assert resource.get('/terms/x/:') == []
        assert resource.get('/terms/x/y') == 0
        assert resource.get('/terms/x/y/docs') == []
        assert resource.get('/terms/x/y/docs/counts') == []
        assert resource.get('/terms/x/y/docs/positions') == []
        assert resource.put('/fields/text') == {'index': 'ANALYZED', 'store': 'NO', 'termvector': 'NO'}
        assert resource.put('/fields/name', store='yes', index='not_analyzed')
        assert sorted(resource.get('/fields')) == ['name', 'text']
        assert resource.get('/fields/text')['index'] == 'ANALYZED'
        assert not resource.post('/docs', docs=[{'name': 'sample', 'text': 'hello world'}])
        with assertRaises(httplib.HTTPException, httplib.BAD_REQUEST):
            resource.post('/docs', docs='')
        (directory, count), = resource.get('/').items()
        assert count == 1
        assert resource.get('/docs') == []
        result = resource.get('/search?q=text:hello')
        assert math.isnan(result.pop('maxscore'))
        assert result == {'query': 'text:hello', 'count': 0, 'docs': []}
        assert resource.post('/commit')
        assert resource.get('/docs') == [0]
        assert resource.get('/docs/0') == {'name': 'sample'}
        assert resource.get('/docs/0', fields='missing') == {'missing': None}
        assert resource.get('/docs/0', multifields='name') == {'name': ['sample']}
        assert resource.get('/docs/0', fields='', multifields='missing') == {'missing': []}
        assert resource.get('/terms') == ['name', 'text']
        assert resource.get('/terms', option='unindexed') == []
        assert resource.get('/terms/text') == ['hello', 'world']
        assert resource.get('/terms/text/world') == 1
        with assertRaises(httplib.HTTPException, httplib.BAD_REQUEST):
            resource.get('/terms/text/world~-')
        assert resource.get('/terms/text/world/docs') == [0]
        assert resource.get('/terms/text/world/docs/counts') == [[0, 1]]
        assert resource.get('/terms/text/world/docs/positions') == [[0, [1]]]
        with assertRaises(httplib.HTTPException, httplib.BAD_REQUEST):
            resource.get('/search?count=')
        with assertRaises(httplib.HTTPException, httplib.BAD_REQUEST):
            resource.get('/search', sort='-x,y')
        with assertRaises(httplib.HTTPException, httplib.BAD_REQUEST):
            resource.get('/search', count=1, sort='x:str')
        assert resource.get('/search', count=0) == {'count': 1, 'maxscore': 1.0, 'query': None, 'docs': []}
        assert resource.get('/search', fields='')['docs'] == [{'__id__': 0, '__score__': 1.0}]
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
        hit, = result['docs']
        assert hit['__id__'] == doc['__id__'] and hit['__score__'] > doc['__score__']
        resource = client.Resource('localhost', self.ports[-1])
        assert resource.get('/docs') == []
        assert resource.post('/refresh', filters=True) == 2
        assert resource.get('/docs') == [0, 1]
        with assertRaises(httplib.HTTPException, httplib.NOT_FOUND):
            resource.get('/fields')
        with assertRaises(httplib.HTTPException, httplib.NOT_FOUND):
            resource.post('/commit')
        resource = client.Resource('localhost', self.ports[0])
        assert not resource.delete('/search', q='name:sample')
        assert resource.get('/docs') == [0]
        assert not resource.post('/commit')
        assert resource.get('/docs') == []
        with assertRaises(httplib.HTTPException, httplib.MOVED_PERMANENTLY):
            resource.post('/refresh', spellcheckers=True)
        resource = client.Resource('localhost', self.ports[-1] + 1)
        with assertRaises(socket.error, errno.ECONNREFUSED):
            resource.get('/')
    
    def testBasic(self):
        "Remote text indexing and searching."
        resource = client.Resource('localhost', self.ports[0])
        assert resource.get('/fields') == []
        for name, settings in fixture.constitution.fields.items():
            assert resource.put('/fields/' + name, **settings)
        fields = resource.get('/fields')
        assert sorted(fields) == ['amendment', 'article', 'date', 'text']
        for field in fields:
            assert sorted(resource.get('/fields/' + name)) == ['index', 'store', 'termvector']
        resource.post('/docs/', docs=list(fixture.constitution.docs()))
        assert resource.get('/').values() == [35]
        resource.post('/commit', spellcheckers=True, filters='')
        assert resource.get('/terms') == ['amendment', 'article', 'date', 'text']
        articles = resource.get('/terms/article')
        articles.remove('Preamble')
        assert sorted(map(int, articles)) == range(1, 8)
        assert sorted(map(int, resource.get('/terms/amendment'))) == range(1, 28)
        assert resource.get('/terms/text/:0') == []
        assert resource.get('/terms/text/z:') == []
        assert resource.get('/terms/text/right:right~') == resource.get('/terms/text/right*') == ['right', 'rights']
        assert resource.get('/terms/text/writ%3f') == ['writs']
        assert resource.get('/terms/text/writ*') == ['writ', 'writing', 'writings', 'writs', 'written']
        assert resource.get('/terms/text/*?count=0') == []
        assert resource.get('/terms/text/writ*?count=10') == ['writs', 'writ', 'writing', 'writings', 'written']
        assert resource.get('/terms/text/writ*?count=3') == ['writs', 'writ', 'writing']
        assert resource.get('/terms/text/right~') == resource.get('/terms/text/right~0.5') == ['eight', 'right', 'rights']
        assert resource.get('/terms/text/right~?count=3') == ['right', 'eight', 'rights']
        assert resource.get('/terms/text/right~?count=5') == ['right', 'eight', 'rights', 'high']
        assert resource.get('/terms/text/write~?count=5') == ['writs', 'writ', 'crime', 'written']
        docs = resource.get('/terms/text/people/docs')
        assert resource.get('/terms/text/people') == len(docs) == 8
        counts = dict(resource.get('/terms/text/people/docs/counts'))
        assert sorted(counts) == docs and all(counts.values()) and sum(counts.values()) > len(counts)
        positions = dict(resource.get('/terms/text/people/docs/positions'))
        assert sorted(positions) == docs and list(map(len, positions.values())) == counts.values()
        result = resource.get('/search', **{'q.field': 'text', 'q': 'write "hello world"', 'spellcheck': 3})
        terms = result['spellcheck'].pop('text')
        assert result['docs'] == [] and result['spellcheck'] == {}
        assert terms == {'write': ['writs', 'writ', 'crime'], 'world': ['would', 'hold', 'gold'], 'hello': ['held', 'well']}
        result = resource.get('/search', **{'q.field': 'text', 'q': 'write "hello world"', 'q.spellcheck': 'true'})
        assert result['query'] == 'text:writs text:"held would"'
        assert result['count'] == len(result['docs']) == resource.get('/terms/text/writs') == 2
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
        assert docs[:5] == result['docs'] and result['count'] == len(docs)
        result = resource.get('/search', q='text:people', count=5, sort='-amendment:int')
        assert math.isnan(result['maxscore']) and all(math.isnan(doc['__score__']) for doc in result['docs'])
        assert [doc['amendment'] for doc in result['docs']] == ['17', '10', '9', '4', '2']
        result = resource.get('/search', q='text:people', sort='-amendment:int')
        assert [doc.get('amendment') for doc in result['docs']] == ['17', '10', '9', '4', '2', '1', None, None]
        assert result == resource.get('/search', q='text:people', sort='-date,-amendment:int')
        maxscore = result['maxscore']
        assert maxscore == max(doc['__score__'] for doc in result['docs'])
        result = resource.get('/search', q='text:people', count=5, sort='-amendment:int', **{'sort.scores': ''})
        assert math.isnan(result['maxscore']) and maxscore in (doc['__score__'] for doc in result['docs'])
        result = resource.get('/search', q='text:people', count=1, sort='-amendment:int', **{'sort.scores': 'max'})
        assert maxscore == result['maxscore'] and maxscore not in (doc['__score__'] for doc in result['docs'])
        result = resource.get('/search', q='text:people', count=5, sort='-article,amendment:int')
        assert [doc.get('amendment') for doc in result['docs']] == [None, None, '1', '2', '4']
        with assertRaises(httplib.HTTPException, httplib.BAD_REQUEST):
            resource.get('/search', q='text:people', sort='-article,amendment:int')
        result = resource.get('/search', q='text:people', start=2, count=2, facets='article,amendment')
        assert [doc['amendment'] for doc in result['docs']] == ['10', '1']
        assert result['count'] == sum(sum(facets.values()) for facets in result['facets'].values())
        for name, keys in [('article', ['1', 'Preamble']), ('amendment', ['1', '10', '17', '2', '4', '9'])]:
            assert sorted(key for key, value in result['facets'][name].items() if value) == keys
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
        assert result['count'] == 25 and result['query'] == 'text:united text:states'
        assert [doc['amendment'] for doc in result['docs'][:4]] == ['10', '11', '15', '19']
        result = resource.get('/search', q='amendment:2', mlt=0, **{'mlt.fields': 'text', 'mlt.minTermFreq': 1, 'mlt.minWordLen': 6})
        assert result['count'] == 11 and result['query'] == 'text:necessary text:people'
        assert [doc['amendment'] for doc in result['docs'][:4]] == ['2', '9', '10', '1']
        result = resource.get('/search', q='text:people', count=1, timeout=-1)
        assert result == {'query': 'text:people', 'count': None, 'maxscore': None, 'docs': []}
        result = resource.get('/search', q='text:people', timeout=0.01)
        assert result['count'] in (None, 8) and result['maxscore'] in (None, maxscore)
        result = resource.get('/search', filter='text:people')
        assert result['count'] == 8 and set(doc['__score__'] for doc in result['docs']) == set([1.0])
        result = resource.get('/search', q='text:right', filter='text:people')
        assert result['count'] == 4 and 0 < result['maxscore'] < 1.0

if __name__ == '__main__':
    lucene.initVM(lucene.CLASSPATH)
    unittest.main()
