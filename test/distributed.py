from future_builtins import map
import unittest
import heapq
import time
import socket, httplib
from lupyne import client
import local, remote

class TestCase(remote.BaseTest):
    ports = 8080, 8081, 8082
    hosts = list(map('localhost:{0:d}'.format, ports))
    def setUp(self):
        local.BaseTest.setUp(self)
        self.servers = list(map(self.start, self.ports))
    
    def testInterface(self):
        "Distributed reading and writing."
        resources = client.Resources(self.hosts, limit=1)
        assert resources.unicast('GET', '/')
        assert not resources.unicast('POST', '/')
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
        assert all(response() is None for response in responses)
        response = resources.unicast('POST', '/docs', {'docs': [doc]})
        assert response() is None
        responses = resources.broadcast('POST', '/commit')
        assert all(response() >= 1 for response in responses)
        responses = resources.broadcast('GET', '/search?q=text:hello')
        docs = []
        for response in responses:
            result = response()
            assert result['count'] >= 1
            docs += result['docs']
        assert len(docs) == len(resources) + 1
        assert len(set(doc['__id__'] for doc in docs)) == 2
        self.stop(self.servers.pop(0))
        self.assertRaises((socket.error, httplib.BadStatusLine), resources.broadcast, 'GET', '/')
        assert resources.unicast('GET', '/')()
        del resources[self.hosts[0]]
        assert all(resources.broadcast('GET', '/'))
        assert list(map(len, resources.values())) == [1, 1]
        time.sleep(self.config['server.socket_timeout'] + 1)
        assert resources.unicast('GET', '/')
        counts = list(map(len, resources.values()))
        assert set(counts) == set([0, 1])
        assert resources.broadcast('GET', '/')
        assert list(map(len, resources.values())) == counts[::-1]
        resources.clear()
        self.assertRaises(ValueError, resources.unicast, 'GET', '/')
    
    def testSharding(self):
        "Sharding of indices across servers."
        keys = range(len(self.hosts))
        shards = client.Shards(zip(self.hosts * 2, heapq.merge(keys, keys)), limit=1)
        shards.resources.broadcast('PUT', '/fields/zone', {'store': 'yes'})
        for zone in range(len(self.ports)):
            shards.broadcast(zone, 'POST', '/docs', {'docs': [{'zone': str(zone)}]})
        shards.resources.broadcast('POST', '/commit')
        result = shards.unicast(0, 'GET', '/search?q=zone:0')()
        assert result['count'] == len(result['docs']) == 1
        assert all(response() == result for response in shards.broadcast(0, 'GET', '/search?q=zone:0'))
        response, = shards.multicast([0], 'GET', '/search')
        assert set(doc['zone'] for doc in response()['docs']) > set('0')
        response, = shards.multicast([0, 1], 'GET', '/search')
        assert set(doc['zone'] for doc in response()['docs']) == set('01')
        zones = set()
        responses = shards.multicast([0, 1, 2], 'GET', '/search')
        assert len(responses) == 2
        for response in responses:
            docs = response()['docs']
            assert len(docs) == 2
            zones.update(doc['zone'] for doc in docs)
        assert zones == set('012')
        self.stop(self.servers.pop(0))
        self.assertRaises((socket.error, httplib.BadStatusLine), shards.broadcast, 0, 'GET', '/')
        responses = shards.multicast([0, 1, 2], 'GET', '/')
        assert len(responses) == 2 and all(response() for response in responses)

if __name__ == '__main__':
    unittest.main()
