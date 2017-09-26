"""
Restful json clients.

Use `Resource`_ for a connection to a single host.
"""

import io
import gzip
import shutil
import httplib
from .utils import json


class Response(httplib.HTTPResponse):
    "A completed response which handles json and caches its body."
    content_type = 'application/json'

    def end(self):
        self.body = self.read()
        if 'gzip' in self.getheader('content-encoding', ''):
            self.body = gzip.GzipFile(fileobj=io.BytesIO(self.body)).read()

    def __nonzero__(self):
        "Return whether status is successful."
        return httplib.OK <= self.status < httplib.MULTIPLE_CHOICES

    def __call__(self):
        "Return evaluated response body or raise exception."
        body = self.body
        if body and self.getheader('content-type').startswith(self.content_type):
            body = json.loads(body)
        if self:
            return body
        raise httplib.HTTPException(self.status, self.reason, body)


class Resource(httplib.HTTPConnection):
    "Synchronous connection which handles json responses."
    response_class = Response
    headers = {'accept-encoding': 'compress, gzip', 'content-length': '0'}

    def request(self, method, path, body=None):
        "Send request after handling body and headers."
        httplib.HTTPConnection.request(self, method, path, body, self.headers)

    def getresponse(self, filename=''):
        "Return completed response, optionally write response body to a file."
        response = httplib.HTTPConnection.getresponse(self)
        if response and filename:
            with open(filename, 'w') as output:
                shutil.copyfileobj(response, output)
        response.end()
        return response

    def call(self, method, path, body=None, params=(), redirect=False):
        "Send request and return completed `response`_."
        self.request(method, path, body)
        return self.getresponse()

    def download(self, path, filename):
        "Download response body from GET request to a file."
        self.request('GET', path)
        return self.getresponse(filename)()

    def delete(self, path, **params):
        "Return response body from DELETE request."
        return self.call('DELETE', path, params=params)()
