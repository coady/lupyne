"""
Restful json clients.

Use `Resource`_ for a single host.
Use `Resources`_ for multiple hosts with simple partitioning or redundancy.
Use `Shards`_ for horizontally partitioning hosts by different keys.

`Resources`_ optionally reuse connections, handling request timeouts.
Broadcasting to multiple resources is parallelized with asynchronous requests and responses.

The load balancing strategy is randomized, biased by the number of cached connections available.
This inherently provides limited failover support, but applications must still handle exceptions as desired.
"""

from future_builtins import map
import warnings
import random
import itertools
import collections
import io, gzip
import httplib, urllib, urlparse
import socket, errno
try:
    import simplejson as json
except ImportError:
    import json

class Response(httplib.HTTPResponse):
    "A completed response which handles json and caches its body."
    def begin(self):
        httplib.HTTPResponse.begin(self)
        self.body = self.read()
        self.close()
        self.time = float(self.getheader('x-response-time', 'nan'))
        if 'gzip' in self.getheader('content-encoding', ''):
            self.body = gzip.GzipFile(fileobj=io.BytesIO(self.body)).read()
    def __nonzero__(self):
        "Return whether status is successful."
        return httplib.OK <= self.status < httplib.MULTIPLE_CHOICES
    def __call__(self):
        "Return evaluated response body or raise exception."
        body = self.body
        if body and self.getheader('content-type').endswith('json'):
            body = json.loads(body)
        code, agent, text = self.getheader('warning', '  ').split(' ', 2)
        if agent == 'lupyne':
            warnings.warn(json.loads(text))
        if self:
            return body
        raise httplib.HTTPException(self.status, self.reason, body)

class Resource(httplib.HTTPConnection):
    "Synchronous connection which handles json responses."
    response_class = Response
    headers = {'accept-encoding': 'compress, gzip', 'content-length': '0'}
    def request(self, method, path, body=None):
        "Send request after handling body and headers."
        headers = dict(self.headers)
        if body is not None:
            body = json.dumps(body)
            headers.update({'content-length': str(len(body)), 'content-type': 'application/json'})
        httplib.HTTPConnection.request(self, method, path, body, headers)
    def call(self, method, path, body=None, params=(), redirect=False, **kwargs):
        "Send request and return completed `response`_."
        if params:
            path += '?' + urllib.urlencode(params, doseq=True)
        if kwargs:
            body = kwargs
            warnings.warn('Keyword arguments deprecated; call with body param instead.', DeprecationWarning)
        self.request(method, path, body)
        response = self.getresponse()
        if redirect and httplib.MULTIPLE_CHOICES <= response.status < httplib.BAD_REQUEST:
            url = urlparse.urlparse(response.getheader('location'))
            assert url.netloc.startswith(self.host)
            warnings.warn('{0}: {1}'.format(response.reason, url.path), DeprecationWarning)
            return self.call(method, url.path, body, params, redirect-1)
        return response
    def multicall(self, *requests):
        "Pipeline requests (method, path[, body]) and return completed responses."
        responses = []
        for request in requests:
            self.request(*request)
            responses.append(self.response_class(self.sock, self.debuglevel, self.strict, self._method))
            self._HTTPConnection__state = 'Idle'
        for response in responses:
            response.begin()
        return responses
    def get(self, path, **params):
        return self.call('GET', path, params=params)()
    def post(self, path, body=None, **kwargs):
        return self.call('POST', path, body, **kwargs)()
    def put(self, path, body=None, **kwargs):
        return self.call('PUT', path, body, **kwargs)()
    def delete(self, path, **params):
        return self.call('DELETE', path, params=params)()

class Resources(dict):
    """Thread-safe mapping of hosts to optionally persistent resources.
    
    :param hosts: host[:port] strings
    :param limit: maximum number of cached connections per host
    """
    class queue(collections.deque):
        "Queue of prioritized resources."
    def __init__(self, hosts, limit=0):
        self.update((host, self.queue(maxlen=limit)) for host in hosts)
    def request(self, host, method, path, body=None):
        "Send request to given host and return exclusive `resource`_."
        try:
            resource = self[host].popleft()
        except IndexError:
            resource = Resource(host)
        resource.request(method, path, body)
        return resource
    def getresponse(self, host, resource):
        """Return `response`_ and release `resource`_ if request completed.
        Return None if it appears request may be repeated."""
        try:
            response = resource.getresponse()
        except httplib.BadStatusLine:
            pass
        except socket.error as exc:
            if exc.errno != errno.ECONNRESET:
                raise
        else:
            if response.status != httplib.REQUEST_TIMEOUT and (response.status != httplib.BAD_REQUEST or response.body != 'Illegal end of headers.'):
                self[host].append(resource)
                return response
        resource.close()
    def priority(self, host):
        "Return priority for host.  None may be used to eliminate from consideration."
        return -len(self[host])
    def choice(self, hosts):
        "Return chosen host according to priority."
        priorities = collections.defaultdict(list)
        for host in hosts:
            priorities[self.priority(host)].append(host)
        priorities.pop(None, None)
        return random.choice(priorities[min(priorities)])
    def unicast(self, method, path, body=None, hosts=()):
        "Send request and return `response`_ from any host, optionally from given subset."
        host = self.choice(tuple(hosts) or self)
        resource = self.request(host, method, path, body)
        response = self.getresponse(host, resource)
        return resource.call(method, path, body) if response is None else response
    def broadcast(self, method, path, body=None, hosts=()):
        "Send requests and return responses from all hosts, optionally from given subset."
        hosts = tuple(hosts) or self
        resources = [self.request(host, method, path, body) for host in hosts]
        responses = list(map(self.getresponse, hosts, resources))
        indices = [index for index, response in enumerate(responses) if response is None]
        for index in indices:
            resources[index].request(method, path, body)
        for index in indices:
            responses[index] = resources[index].getresponse()
        return responses

class Shards(dict):
    """Mapping of keys to host clusters, with associated `resources`_.
    
    :param items: host, key pairs
    :param limit: maximum number of cached connections per host
    :param multimap: mapping of hosts to multiple keys
    """
    choice = Resources.__dict__['choice']
    def __init__(self, items=(), limit=0, **multimap):
        pairs = ((host, key) for host in multimap for key in multimap[host])
        for host, key in itertools.chain(items, pairs):
            self.setdefault(key, set()).add(host)
        self.resources = Resources(itertools.chain(*self.values()), limit)
    def priority(self, hosts):
        "Return combined priority for hosts."
        priorities = list(map(self.resources.priority, hosts))
        if None not in priorities:
            return len(priorities), sum(priorities)
    def unicast(self, key, method, path, body=None):
        "Send request and return `response`_ from any host for corresponding key."
        return self.resources.unicast(method, path, body, self[key])
    def broadcast(self, key, method, path, body=None):
        "Send requests and return responses from all hosts for corresponding key."
        return self.resources.broadcast(method, path, body, self[key])
    def multicast(self, keys, method, path, body=None):
        """Send requests and return responses from a minimal subset of hosts which cover all corresponding keys.
        Response overlap is possible depending on partitioning.
        """
        shards = frozenset(),
        for key in keys:
            shards = set(hosts.union([host]) for hosts, host in itertools.product(shards, self[key]))
        return self.resources.broadcast(method, path, body, self.choice(shards))
