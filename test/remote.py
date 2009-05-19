import unittest
import sys
import subprocess
import operator
import httplib
import cherrypy
from lupyne import client
import fixture
from local import BaseTest

class TestCase(BaseTest):
    host = 'localhost:8080'
    def setUp(self):
        BaseTest.setUp(self)
        stderr = None if self.verbose else subprocess.PIPE
        host, port = self.host.split(':')
        cherrypy.process.servers.wait_for_free_port(host, port)
        self.server = subprocess.Popen([sys.executable, '-m', 'lupyne.server', self.tempdir], stderr=stderr)
        cherrypy.process.servers.wait_for_occupied_port(host, port)
        assert self.server.poll() is None
    def tearDown(self):
        self.server.terminate()
        assert self.server.wait() == 0
        BaseTest.tearDown(self)
    
    def testInterface(self):
        "Remote reading and writing."
        resource = client.Resource(self.host)
        self.assertRaises(ValueError, resource.get, '/favicon.ico')
        (directory, count), = resource.get('/').items()
        assert count == 0 and directory.startswith('org.apache.lucene.store.FSDirectory@')
        assert resource.get('/docs') == []
        self.assertRaises(httplib.HTTPException, resource.get, '/docs/0')
        assert resource.get('/fields') == []
        self.assertRaises(httplib.HTTPException, resource.get, '/fields/name')
        assert resource.get('/terms') == []
        assert resource.get('/terms/x') == []
        assert resource.get('/terms/x/:') == []
        assert resource.get('/terms/x/y') == 0
        assert resource.get('/terms/x/y/docs') == []
        assert resource.get('/terms/x/y/docs/counts') == []
        assert resource.get('/terms/x/y/docs/positions') == []
        resource.put('/fields/text')
        resource.put('/fields/name', store='yes', index='not_analyzed')
        assert sorted(resource.get('/fields')) == ['name', 'text']
        assert resource.get('/fields/text')['index'] == 'ANALYZED'
        resource.post('/docs', docs=[{'name': 'sample', 'text': 'hello world'}])
        (directory, count), = resource.get('/').items()
        assert count == 1
        assert resource.get('/docs') == []
        assert resource.get('/search/?q=text:hello') == {'count': 0, 'docs': []}
        resource.post('/commit')
        assert resource.get('/docs') == [0]
        assert resource.get('/docs/0') == {'name': 'sample'}
        assert resource.get('/docs/0?fields=missing') == {'missing': None}
        assert resource.get('/docs/0?multifields=name') == {'name': ['sample']}
        assert resource.get('/terms') == ['name', 'text']
        assert resource.get('/terms?option=unindexed') == []
        assert resource.get('/terms/text') == ['hello', 'world']
        assert resource.get('/terms/text/world') == 1
        assert resource.get('/terms/text/world/docs') == [0]
        assert resource.get('/terms/text/world/docs/counts') == [[0, 1]]
        assert resource.get('/terms/text/world/docs/positions') == [[0, [1]]]
        hits = resource.get('/search', q='text:hello')
        assert sorted(hits) == ['count', 'docs']
        assert hits['count'] == 1
        doc, = hits['docs']
        assert sorted(doc) == ['__id__', '__score__', 'name']
        assert doc['__id__'] == 0 and doc['__score__'] > 0 and doc['name'] == 'sample' 
        resource.delete('/search/?q=name:sample')
        assert resource.get('/docs') == [0]
        resource.post('/commit')
        assert resource.get('/docs') == []
    
    def testBasic(self):
        "Remote text indexing and searching."
        resource = client.Resource(self.host)
        assert resource.get('/fields') == []
        for name, settings in fixture.constitution.fields.items():
            assert resource.put('/fields/' + name, **settings)
        fields = resource.get('/fields')
        assert sorted(fields) == ['amendment', 'article', 'date', 'text']
        for field in fields:
            assert sorted(resource.get('/fields/' + name)) == ['index', 'store', 'termvector']
        resource.post('/docs/', docs=list(fixture.constitution.docs()))
        assert resource.get('/').values() == [35]
        resource.post('/commit')
        assert resource.get('/terms') == ['amendment', 'article', 'date', 'text']
        articles = resource.get('/terms/article')
        articles.remove('Preamble')
        assert sorted(map(int, articles)) == range(1, 8)
        assert sorted(map(int, resource.get('/terms/amendment'))) == range(1, 28)
        assert resource.get('/terms/text/:0') == []
        assert resource.get('/terms/text/z:') == []
        assert resource.get('/terms/text/right:right~') == ['right', 'rights']
        docs = resource.get('/terms/text/people/docs')
        assert resource.get('/terms/text/people') == len(docs) == 8
        counts = dict(resource.get('/terms/text/people/docs/counts'))
        assert sorted(counts) == docs and all(counts.values()) and sum(counts.values()) > len(counts)
        positions = dict(resource.get('/terms/text/people/docs/positions'))
        assert sorted(positions) == docs and map(len, positions.values()) == counts.values()
        result = resource.get('/search', q='text:"We the People"')
        assert sorted(result) == ['count', 'docs'] and result['count'] == 1
        doc, = result['docs']
        assert sorted(doc) == ['__id__', '__score__', 'article']
        assert doc['article'] == 'Preamble' and doc['__id__'] >= 0 and 0 < doc['__score__'] < 1
        result = resource.get('/search', q='text:people')
        docs = result['docs']
        assert sorted(docs, key=operator.itemgetter('__score__'), reverse=True) == docs
        assert len(docs) == result['count'] == 8
        result = resource.get('/search', q='text:people', count=5)
        assert docs[:5] == result['docs'] and result['count'] == len(docs)
        result = resource.get('/search', q='text:freedom')
        assert result['count'] == 1
        doc, = result['docs']
        assert doc['amendment'] == '1'

if __name__ == '__main__':
    unittest.main()
