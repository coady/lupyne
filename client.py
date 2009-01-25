"""
Restful json client.
"""

import os
import json
import httplib, urllib

class Resource(httplib.HTTPConnection):
    "Synchronous connection which handles json responses."
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/json"}
    def __call__(self, method, path, body=None, headers=()):
        headers = dict(headers)
        if body is not None:
            body = urllib.urlencode(dict((name, value if isinstance(value, basestring) else json.dumps(value)) \
                for name, value in body.items()))
            headers['Content-Length'] = len(body)
        self.request(method, path, body, headers)
        response = self.getresponse()
        if response.status == httplib.OK:
            return json.load(response)
        raise httplib.HTTPException(response.status, response.read())
    def get(self, path='/', **params):
        if params:
            path += '?' + urllib.urlencode(params)
        return self('GET', path)
    def post(self, path, **params):
        return self('POST', path, params, self.headers)
    def put(self, path, **params):
        return self('PUT', path, params, self.headers)
    def delete(self, path):
        return self('DELETE', path)
