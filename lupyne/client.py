"""
Restful json client.
"""

import gzip
import random
import itertools
from cStringIO import StringIO
import httplib, urllib
try:
    import simplejson as json
except ImportError:
    import json

class Response(httplib.HTTPResponse):
    "A closed response which handles json and caches its body."
    def begin(self):
        httplib.HTTPResponse.begin(self)
        self.body = self.read()
        if self.getheader('content-encoding') == 'gzip':
            self.body = gzip.GzipFile(fileobj=StringIO(self.body)).read()
    def __call__(self):
        "Return evaluated response body or raise exception."
        if not httplib.OK <= self.status < httplib.MULTIPLE_CHOICES:
            raise httplib.HTTPException(self.status, self.reason, self.body)
        if self.body and self.getheader('content-type') == 'text/x-json':
            return json.loads(self.body)
        return self.body

class Resource(httplib.HTTPConnection):
    "Synchronous connection which handles json responses."
    response_class = Response
    def request(self, method, path, body=None):
        "Send request after handling body and headers."
        headers = {'accept-encoding': 'compress, gzip'}
        if body is not None:
            body = urllib.urlencode(dict((name, value if isinstance(value, basestring) else json.dumps(value)) \
                for name, value in body.items()))
            headers.update({'content-length': len(body), 'content-type': 'application/x-www-form-urlencoded'})
        httplib.HTTPConnection.request(self, method, path, body, headers)
    def __call__(self, method, path, body=None):
        "Send request and return evaluated response body."
        self.request(method, path, body)
        return self.getresponse()()
    def get(self, path='/', **params):
        if params:
            path += '?' + urllib.urlencode(params)
        return self('GET', path)
    def post(self, path, **body):
        return self('POST', path, body)
    def put(self, path, **body):
        return self('PUT', path, body)
    def delete(self, path):
        return self('DELETE', path)

class Resources(dict):
    "Persistent thread-safe http connections with support for redundancy and partitioning."
    def __init__(self, hosts):
        self.update((host, []) for host in hosts)
    def pop(self, host):
        "Return an exclusive resource for given host."
        try:
            return self[host].pop()
        except IndexError:
            return Resource(host)
    def push(self, host, resource):
        "Put resource back into available pool."
        self[host].append(resource)
    def request(self, host, method, path, body=None):
        "Send request to given host and return resource."
        resource = self.pop(host)
        resource.request(method, path, body)
        return resource
    def unicast(self, method, path, body=None, hosts=()):
        "Send request and return response from any host, optionally from given subset."
        hosts = list(hosts) or self
        candidates = itertools.chain.from_iterable([host] * len(self[host]) for host in hosts)
        host = random.choice(list(candidates) or list(hosts))
        resource = self.request(host, method, path, body)
        response = resource.getresponse()
        if response.status == httplib.REQUEST_TIMEOUT:
            return self.unicast(method, path, body, hosts)
        self.push(host, resource)
        return response
    def broadcast(self, method, path, body=None, hosts=()):
        "Send requests and return responses from all hosts, optionally from given subset."
        hosts = list(hosts) or self
        responses = {}
        resources = dict((host, self.request(host, method, path, body)) for host in hosts)
        while resources:
            for host in list(resources):
                resource = resources.pop(host)
                response = responses[host] = resource.getresponse()
                if response.status == httplib.REQUEST_TIMEOUT:
                    resources[host] = self.request(host, method, path, body)
                else:
                    self.push(host, resource)
        return map(responses.__getitem__, hosts)
