import os
import sys
import subprocess
import time
import httplib
import cherrypy
from lupyne import client, server
from .test_remote import servers  # noqa


def test_replication(tempdir, servers):  # noqa
    "Replication from indexer to searcher."
    directory = os.path.join(tempdir, 'backup')
    sync, update = '--autosync=' + servers.hosts[0], '--autoupdate=1'
    servers.start(servers.ports[0], tempdir),
    servers.start(servers.ports[1], '-r', directory, sync, update),
    servers.start(servers.ports[2], '-r', directory),
    for args in [('-r', tempdir), (update, tempdir), (update, tempdir, tempdir)]:
        assert subprocess.call((sys.executable, '-m', 'lupyne.server', sync) + args, stderr=subprocess.PIPE)
    primary = client.Resource(servers.hosts[0])
    primary.post('/docs', [{}])
    assert primary.post('/update') == 1
    resource = client.Resource(servers.hosts[2])
    response = resource.call('POST', '/', {'host': servers.hosts[0]})
    assert response.status == httplib.ACCEPTED and sum(response().values()) == 0
    assert resource.post('/update') == 1
    assert resource.post('/', {'host': servers.hosts[0], 'path': '/'})
    assert resource.post('/update') == 1
    primary.post('/docs', [{}])
    assert primary.post('/update') == 2
    resource = client.Resource(servers.hosts[1])
    time.sleep(1.1)
    assert sum(resource.get('/').values()) == 2
    servers.stop(servers.ports[-1])
    root = server.WebSearcher(directory, hosts=servers.hosts[:2])
    app = server.mount(root)
    root.fields = {}
    assert root.update() == 2
    assert len(root.hosts) == 2
    servers.stop(servers.ports[0])
    assert resource.get('/docs')
    assert resource.call('POST', '/docs', []).status == httplib.METHOD_NOT_ALLOWED
    assert resource.get('/terms', option='indexed') == []
    assert root.update() == 2
    assert len(root.hosts) == 1
    servers.stop(servers.ports[1])
    assert root.update() == 2
    assert len(root.hosts) == 0 and isinstance(app.root, server.WebIndexer)
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
