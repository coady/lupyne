from future_builtins import map
import unittest
import os
import sys, subprocess
import heapq
import time
import socket, httplib
import lucene, cherrypy
from lupyne import client, server
from . import remote

def getresponse(error):
    "Test error handling in resources."
    raise error(0)

class TestCase(remote.BaseTest):
    ports = 8080, 8081, 8082
    hosts = list(map('localhost:{0:d}'.format, ports))
    
    def testInterface(self):
        "Distributed reading and writing."
        for port in self.ports:
            self.start(port)
        resources = client.Resources(self.hosts, limit=1)
        assert resources.unicast('GET', '/')
        assert not resources.unicast('POST', '/terms')
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
        responses = resources.broadcast('POST', '/docs', [doc])
        assert all(response() is None for response in responses)
        response = resources.unicast('POST', '/docs', [doc])
        assert response() is None
        responses = resources.broadcast('POST', '/update')
        assert all(response() >= 1 for response in responses)
        responses = resources.broadcast('GET', '/search?q=text:hello')
        docs = []
        for response in responses:
            result = response()
            assert result['count'] >= 1
            docs += result['docs']
        assert len(docs) == len(resources) + 1
        assert len(set(doc['__id__'] for doc in docs)) == 2
        self.stop(self.ports[0])
        self.assertRaises(socket.error, resources.broadcast, 'GET', '/')
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
        host = self.hosts[1]
        stream = resources[host].stream('GET', '/')
        resource = next(stream)
        resource.getresponse = lambda: getresponse(socket.error)
        self.assertRaises(socket.error, next, stream)
        stream = resources[host].stream('GET', '/')
        resource = next(stream)
        resource.getresponse = lambda: getresponse(httplib.BadStatusLine)
        assert next(stream) is None
        resources.clear()
        self.assertRaises(ValueError, resources.unicast, 'GET', '/')
        if hasattr(client, 'SResource'):
            client.Pool.resource_class = client.SResource
            self.assertRaises(httplib.ssl.SSLError, client.Pool(self.hosts[-1]).call, 'GET', '/')
        client.Pool.resource_class = client.Resource
    
    def testSharding(self):
        "Sharding of indices across servers."
        for port in self.ports:
            self.start(port)
        keys = range(len(self.hosts))
        shards = client.Shards(zip(self.hosts * 2, heapq.merge(keys, keys)), limit=1)
        shards.resources.broadcast('PUT', '/fields/zone', {'store': 'yes'})
        for zone in range(len(self.ports)):
            shards.broadcast(zone, 'POST', '/docs', [{'zone': str(zone)}])
        shards.resources.broadcast('POST', '/update')
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
        self.stop(self.ports[0])
        self.assertRaises(socket.error, shards.broadcast, 0, 'GET', '/')
        responses = shards.multicast([0, 1, 2], 'GET', '/')
        assert len(responses) == 2 and all(response() for response in responses)
        shards.resources.priority = lambda hosts: None
        self.assertRaises(ValueError, shards.choice, [[0]])
    
    def testReplication(self):
        "Replication from indexer to searcher."
        directory = os.path.join(self.tempdir, 'backup')
        sync, update = '--autosync=' + self.hosts[0], '--autoupdate=1'
        self.start(self.ports[0], self.tempdir),
        self.start(self.ports[1], '-r', directory, sync, update),
        self.start(self.ports[2], '-r', directory),
        for args in [('-r', self.tempdir), (update, self.tempdir), (update, self.tempdir, self.tempdir)]:
            assert subprocess.call((sys.executable, '-m', 'lupyne.server', sync) + args, stderr=subprocess.PIPE)
        replicas = client.Replicas(self.hosts[:2], limit=1)
        replicas.discard(None)
        replicas.post('/docs', [{}])
        assert replicas.post('/update') == 1
        resource = client.Resource(self.hosts[2])
        response = resource.call('POST', '/', {'host': self.hosts[0]})
        assert response.status == httplib.ACCEPTED and sum(response().values()) == 0
        assert resource.post('/update') == 1
        assert resource.post('/', {'host': self.hosts[0], 'path': '/'})
        assert resource.post('/update') == 1
        replicas.post('/docs', [{}])
        assert replicas.post('/update') == 2
        resource = client.Resource(self.hosts[1])
        time.sleep(1.1)
        assert sum(resource.get('/').values()) == 2
        self.stop(self.ports[-1])
        root = server.WebSearcher(directory, hosts=self.hosts[:2])
        app = server.mount(root)
        root.fields = {}
        assert root.update() == 2
        assert len(root.hosts) == 2
        self.stop(self.ports[0])
        assert replicas.get('/docs')
        assert replicas.call('POST', '/docs', []).status == httplib.METHOD_NOT_ALLOWED
        assert replicas.get('/terms', option='indexed') == []
        assert replicas.call('POST', '/docs', [], retry=True).status == httplib.METHOD_NOT_ALLOWED
        assert root.update() == 2
        assert len(root.hosts) == 1
        self.stop(self.ports[1])
        assert root.update() == 2
        assert len(root.hosts) == 0 and isinstance(app.root, server.WebIndexer)
        app.root.close()
        root = server.WebSearcher(directory)
        app = server.mount(root, autoupdate=0.1)
        root.fields, root.autoupdate = {}, 0.1
        cherrypy.config['log.screen'] = self.config['log.screen']
        cherrypy.engine.state = cherrypy.engine.states.STARTED
        root.monitor.start() # simulate engine starting
        time.sleep(0.2)
        app.root.indexer.add()
        time.sleep(0.2)
        assert len(app.root.indexer) == len(root.searcher) + 1
        app.root.monitor.unsubscribe()
        del app.root

if __name__ == '__main__':
    lucene.initVM()
    unittest.main()
