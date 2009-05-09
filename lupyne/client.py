"""
Restful json client.
"""

import gzip
from cStringIO import StringIO
import httplib, urllib
try:
    import simplejson as json
except ImportError:
    import json

class Resource(httplib.HTTPConnection):
    "Synchronous connection which handles json responses."
    def __call__(self, method, path, body=None):
        headers = {'accept-encoding': 'compress, gzip'}
        if body is not None:
            body = urllib.urlencode(dict((name, value if isinstance(value, basestring) else json.dumps(value)) \
                for name, value in body.items()))
            headers.update({'content-length': len(body), 'content-type': 'application/x-www-form-urlencoded'})
        self.request(method, path, body, headers)
        response = self.getresponse()
        if response.status not in (httplib.OK, httplib.CREATED):
            raise httplib.HTTPException(response.status, response.reason, response.read())
        if response.getheader('content-encoding') == 'gzip':
            response = gzip.GzipFile(fileobj=StringIO(response.read()))
        return json.load(response)
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
