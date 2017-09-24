import calendar
import operator
import os
import sys
import subprocess
import time
import httplib
import urllib
from email.utils import parsedate
import cherrypy
import clients
import pytest
from lupyne import engine, server
from .test_remote import servers  # noqa


def test_facets(tempdir, servers, zipcodes):  # noqa
    writer = engine.IndexWriter(tempdir)
    writer.commit()
    servers.start(servers.ports[0], '-r', tempdir)
    writer.set('zipcode', engine.NumericField, type=int, stored=True)
    writer.fields['location'] = engine.NestedField('county.city')
    for doc in zipcodes:
        if doc['state'] == 'CA':
            writer.add(zipcode=doc['zipcode'], location='{}.{}'.format(doc['county'], doc['city']))
    writer.commit()
    resource = clients.Resource(servers.urls[0])
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


def test_queries(servers):  # noqa
    servers.start(servers.ports[0], **{'tools.validate.etag': False})
    resource = clients.Resource(servers.urls[0], headers={'content-length': '0'})
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


def test_realtime(tempdir, servers):  # noqa
    for args in [('-r',), ('--real-time', 'index0', 'index1'), ('-r', '--real-time', 'index')]:
        assert subprocess.call((sys.executable, '-m', 'lupyne.server') + args, stderr=subprocess.PIPE)
    root = server.WebIndexer(tempdir)
    root.indexer.add()
    assert root.update() == 1
    del root
    servers.start(servers.ports[0], '--real-time', **{'tools.validate.expires': 0})
    client = clients.Client(servers.urls[0])
    response = client.get('docs')
    version, modified, expires = map(response.headers.get, ('etag', 'last-modified', 'expires'))
    assert response.ok and modified is None and response.json() == []
    assert client.post('docs', [{}]).status_code == httplib.OK
    response = client.get('docs')
    assert response.ok and response.json() == [0]
    assert client.delete('search').status_code == httplib.OK
    response = client.get('docs')
    assert response.ok and response.json() == []
    time.sleep(max(0, calendar.timegm(parsedate(expires)) + 1 - time.time()))
    assert client.post(json=[tempdir]).status_code == httplib.OK
    response = client.get('docs')
    assert response.ok and response.json() == [0] and expires != response.headers['expires']
    assert client.post('update').ok
    response = client.get('docs')
    assert response.ok and version != response.headers['etag']


def test_example(request, servers):  # noqa
    """Custom server example (only run explicitly)."""
    if request.config.option.verbose < 0:
        pytest.skip("requires verbose output")
    servers.module = 'examples.server'
    servers.start(servers.ports[0])
    resource = clients.Resource(servers.urls[0])
    result = resource.search(q='date:17*', group='year')
    assert dict(map(operator.itemgetter('value', 'count'), result['groups'])) == {1795: 1, 1791: 10}
    result = resource.search(q='date:17*', group='year', sort='-year')
    assert list(map(operator.itemgetter('value'), result['groups'])) == [1795, 1791]
    result = resource.search(count=0, facets='year')
    facets = result['facets']['year']
    assert not result['docs'] and facets['1791'] == 10 and sum(facets.values()) == result['count']
    result = resource.search(q='text:right', facets='year')
    facets = result['facets']['year']
    assert len(result['docs']) == result['count'] == sum(facets.values())


def test_replication(tempdir, servers):  # noqa
    directory = os.path.join(tempdir, 'backup')
    sync, update = '--autosync=' + servers.hosts[0], '--autoupdate=1'
    servers.start(servers.ports[0], tempdir),
    servers.start(servers.ports[1], '-r', directory, sync, update),
    servers.start(servers.ports[2], '-r', directory),
    for args in [('-r', tempdir), (update, tempdir), (update, tempdir, tempdir)]:
        assert subprocess.call((sys.executable, '-m', 'lupyne.server', sync) + args, stderr=subprocess.PIPE)
    primary = clients.Resource(servers.urls[0])
    primary.post('docs', [{}])
    assert primary.post('update') == 1
    resource = clients.Resource(servers.urls[2])
    response = resource.client.post(json={'host': servers.hosts[0]})
    assert response.status_code == httplib.ACCEPTED and sum(response.json().values()) == 0
    assert resource.post('update') == 1
    assert resource.post(json={'host': servers.hosts[0], 'path': '/'})
    assert resource.post('update') == 1
    primary.post('docs', [{}])
    assert primary.post('update') == 2
    resource = clients.Resource(servers.urls[1])
    time.sleep(1.1)
    assert sum(resource().values()) == 2
    servers.stop(servers.ports[-1])
    root = server.WebSearcher(directory, hosts=servers.hosts[:2])
    app = server.mount(root)
    root.fields = {}
    assert root.update() == 2
    assert len(root.hosts) == 2
    servers.stop(servers.ports[0])
    assert resource.docs()
    assert resource.client.post('docs', []).status_code == httplib.METHOD_NOT_ALLOWED
    assert resource.terms(option='indexed') == []
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
