import unittest
from lupyne import client
import remote

class TestCase(remote.BaseTest):
    ports = 8080, 8081
    def setUp(self):
        remote.BaseTest.setUp(self)
        self.servers = map(self.start, self.ports)
    def tearDown(self):
        for server in self.servers:
            self.stop(server)
        remote.BaseTest.tearDown(self)
    
    def testInterface(self):
        "Distributed reading and writing."
        resources = client.Resources(map('localhost:{0:n}'.format, self.ports))
        responses = resources.broadcast('GET', '/')
        assert len(responses) == len(resources)
        for response in responses:
            (directory, count), = response().items()
            assert count == 0 and directory.startswith('org.apache.lucene.store.RAMDirectory@')
        responses = resources.broadcast('PUT', '/fields/text')
        assert all(response() == {'index': 'ANALYZED', 'store': 'NO', 'termvector': 'NO'} for response in responses)
        responses = resources.broadcast('PUT', '/fields/name', {'store': 'yes', 'index': 'not_analyzed'})
        assert all(response() == {'index': 'NOT_ANALYZED', 'store': 'YES', 'termvector': 'NO'} for response in responses)
        doc = {'name': 'sample', 'text': 'hello world'}
        responses = resources.broadcast('POST', '/docs', {'docs': [doc]})
        assert all(response() == '' for response in responses)
        response = resources.unicast('POST', '/docs', {'docs': [doc]})
        assert response() == ''
        responses = resources.broadcast('POST', '/commit')
        assert all(response() >= 1 for response in responses)
        responses = resources.broadcast('GET', '/search/?q=text:hello')
        docs = []
        for response in responses:
            result = response()
            assert result['count'] >= 1
            docs += result['docs']
        assert len(docs) == 3
        assert len(set(doc['__id__'] for doc in docs)) == 2

if __name__ == '__main__':
    unittest.main()
