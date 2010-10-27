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
import random
import itertools
import collections
import io, gzip
import httplib, urllib
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
        if 'gzip' in self.getheader('content-encoding', ''):
            self.body = gzip.GzipFile(fileobj=io.BytesIO(self.body)).read()
    def __nonzero__(self):
        "Return whether status is successful."
        return httplib.OK <= self.status < httplib.MULTIPLE_CHOICES
    def __call__(self):
        "Return evaluated response body or raise exception."
        body = self.body
        if body and 'text/x-json' in self.getheader('content-type'):
            body = json.loads(body)
        if self:
            return body
        raise httplib.HTTPException(self.status, self.reason, body)

class Resource(httplib.HTTPConnection):
    "Synchronous connection which handles json responses."
    response_class = Response
    def request(self, method, path, body=None):
        "Send request after handling body and headers."
        headers = {'accept-encoding': 'compress, gzip', 'content-length': '0'}
        if body is not None:
            body = urllib.urlencode(dict((name, value if isinstance(value, basestring) else json.dumps(value)) \
                for name, value in body.items()))
            headers.update({'content-length': str(len(body)), 'content-type': 'application/x-www-form-urlencoded'})
        httplib.HTTPConnection.request(self, method, path, body, headers)
    def __call__(self, method, path, body=None):
        "Send request and return evaluated response body."
        self.request(method, path, body)
        return self.getresponse()()
    def get(self, path, **params):
        if params:
            path += '?' + urllib.urlencode(params, doseq=True)
        return self('GET', path)
    def post(self, path, **body):
        return self('POST', path, body)
    def put(self, path, **body):
        return self('PUT', path, body)
    def delete(self, path, **params):
        if params:
            path += '?' + urllib.urlencode(params, doseq=True)
        return self('DELETE', path)

class Resources(dict):
    """Thread-safe mapping of hosts to optionally persistent resources.
    
    :param hosts: host[:port] strings
    :param limit: maximum number of cached connections per host
    """
    class Manager(collections.deque):
        "Queue of prioritized resources."
    def __init__(self, hosts, limit=0):
        self.update((host, self.Manager(maxlen=limit)) for host in hosts)
    def request(self, host, method, path, body=None):
        "Send request to given host and return exclusive resource."
        try:
            resource = self[host].popleft()
        except IndexError:
            resource = Resource(host)
        resource.request(method, path, body)
        return resource
    def getresponse(self, host, resource):
        "Return response and release resource if request completed."
        response = resource.getresponse()
        if response.status != httplib.REQUEST_TIMEOUT:
            self[host].append(resource)
            return response
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
        "Send request and return response from any host, optionally from given subset."
        host = self.choice(tuple(hosts) or self)
        resource = self.request(host, method, path, body)
        response = self.getresponse(host, resource)
        if response is None:
            resource.request(method, path, body)
            response = resource.getresponse()
        return response
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
    """Mapping of keys to host clusters, with associated resources.
    
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
        "Send request and return response from any host for corresponding key."
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
