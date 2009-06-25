"""
Restful json `CherryPy <http://cherrypy.org/>`_ server.

    $ python server.py `directory` [-r|--read-only] [-c|--config filename]

CherryPy and Lucene VM integration issues:
 * Autoreload is not compatible with the VM initialization.
 * WorkerThreads must be attached to the VM.
 * Also recommended that the VM ignores keyboard interrupts for clean server shutdown.
"""

try:    # optimization
    import simplejson as json
except ImportError:
    import json
import httplib
import threading
from contextlib import contextmanager
import lucene
import cherrypy
from cherrypy.wsgiserver import WorkerThread
from engine import Indexer, IndexSearcher

def json_tool():
    "Transform responses into json format."
    response = cherrypy.response
    if response.status is None and response.headers['content-type'].startswith('text/'):
        response.headers['content-type'] = 'text/x-json'
        response.body = json.dumps(response.body)
cherrypy.tools.json = cherrypy.Tool('before_finalize', json_tool)

def allow_tool(methods=['GET', 'HEAD']):
    request = cherrypy.request
    if request.method not in methods:
        cherrypy.response.headers['allow'] = ', '.join(methods)
        message = "The path {0!r} does not allow {1}.".format(request.path_info, request.method)
        raise cherrypy.HTTPError(httplib.METHOD_NOT_ALLOWED, message)
cherrypy.tools.allow = cherrypy.Tool('on_start_resource', allow_tool)

class AttachedThread(WorkerThread):
    "Attach cherrypy threads to lucene VM."
    def run(self):
        lucene.getVMEnv().attachCurrentThread()
        WorkerThread.run(self)

@contextmanager
def handleNotFound(exception):
    "Interpret given exception as 404 Not Found."
    try:
        yield
    except exception:
        raise cherrypy.NotFound()

@contextmanager
def handleBadRequest(exception):
    "Interpret given exception as 400 Bad Request."
    try:
        yield
    except exception as exc:
        raise cherrypy.HTTPError(httplib.BAD_REQUEST, str(exc))

class WebSearcher(object):
    "Dispatch root with a delegated IndexSearcher."
    _cp_config = {'tools.json.on': True, 'tools.allow.on': True,
        'tools.gzip.on': True, 'tools.gzip.mime_types': ['text/html', 'text/plain', 'text/x-json']}
    def __init__(self, *args, **kwargs):
        self.indexer = IndexSearcher(*args, **kwargs)
    @cherrypy.expose
    def index(self):
        """Return index information.
        
        **GET** /
            Return a mapping of the directory to the document count.
            
            :return: {*string*: *int*}
        """
        return {str(self.indexer.directory): len(self.indexer)}
    @cherrypy.expose
    def docs(self, id=None, fields='', multifields=''):
        """Return ids or documents.
        
        **GET** /docs
            Return list of doc ids.
            
            :return: [*int*,... ]
        
        **GET** /docs/*int*?
            Return document mappings, optionally selected unique or multi-valued fields.
            
            &fields=\ *chars*,...
            
            &multifields=\ *chars*,...
            
            :return: {*string*: *string*\|\ *array*,... }
        """
        if id is None:
            return list(self.indexer)
        with handleBadRequest(ValueError):
            id = int(id)
        fields = dict.fromkeys(filter(None, fields.split(',')))
        multifields = filter(None, multifields.split(','))
        with handleNotFound(KeyError):
            doc = self.indexer[id]
        return doc.dict(*multifields, **fields)
    @cherrypy.expose
    def search(self, q=None, count=None, fields='', multifields='', sort=None, reverse='false'):
        """Run query and return documents.
        
        **GET** /search?
            Return list of document objects and total doc count.
            
            &q=\ *chars*
            
            &count=\ *int*
            
            &fields=\ *chars*,...
            
            &multifields=\ *chars*,...
            
            &sort=\ *chars*,...
            
            &reverse=true|false,
            
            :return: {"count": *int*, "docs": [{"__id__": *int*, "__score__": *number*, *string*: *string*\|\ *array*,... },... ]}
        """
        with handleBadRequest(ValueError):
            count = count and int(count)
            reverse = json.loads(reverse)
        fields = dict.fromkeys(filter(None, fields.split(',')))
        multifields = filter(None, multifields.split(','))
        if sort is not None and ',' in sort:
            sort = fields.split(',')
        hits = self.indexer.search(q, count=count, sort=sort, reverse=reverse)
        docs = [hit.dict(*multifields, **fields) for hit in hits]
        return {'count': hits.count, 'docs': docs}
    @cherrypy.expose
    def terms(self, name='', value=':', *args, **options):
        """Return data about indexed terms.
        
        **GET** /terms?
            Return field names, with optional selection.
            
            &option=\ *chars*
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*
            Return term values for given field name.
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*/*chars*:*chars*
            Return slice of term values for given field name.
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*/*chars*
            Return document count with given term.
            
            :return: *int*
        
        **GET** /terms/*chars*/*chars*/docs
            Return document ids with given term.
            
            :return: [*int*,... ]
        
        **GET** /terms/*chars*/*chars*/docs/counts
            Return document ids and frequency counts for given term.
            
            :return: [[*int*, *int*],... ]
        
        **GET** /terms/*chars*/*chars*/docs/positions
            Return document ids and positions for given term.
            
            :return: [[*int*, [*int*,... ]],... ]
        """
        if not name:
            return sorted(self.indexer.names(**options))
        if ':' in value:
            start, stop = value.split(':')
            return list(self.indexer.terms(name, start, stop or None))
        docs, stats = (args + ('', ''))[:2]
        if not docs:
            return self.indexer.count(name, value)
        if docs == 'docs':
            if stats == 'positions':
                return list(self.indexer.positions(name, value))
            if stats in ('', 'counts'):
                return list(self.indexer.docs(name, value, counts=bool(stats)))
        raise cherrypy.NotFound()

class WebIndexer(WebSearcher):
    "Dispatch root which extends searcher to include write methods."
    def __init__(self, *args, **kwargs):
        self.indexer = Indexer(*args, **kwargs)
        self.lock = threading.Lock()
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    def commit(self):
        """Commit write operations.
        
        **POST** /commit
        """
        with self.lock:
            self.indexer.commit()
        return len(self.indexer)
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'POST'])
    def docs(self, id=None, fields='', multifields='', docs='[]'):
        """Add or return documents.
        
        **POST** /docs
            Add documents to index.
            
            docs=[{*string*: *string*\|\ *array*,... },... ]
        """
        if cherrypy.request.method != 'POST':
            return WebSearcher.docs(self, id, fields, multifields)
        with handleBadRequest(ValueError):
            docs = json.loads(docs)
        for doc in docs:
            self.indexer.add(doc)
        cherrypy.response.status = httplib.ACCEPTED
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'DELETE'])
    def search(self, q=None, count=None, fields='', multifields='', sort=None, reverse='false'):
        """Run or delete a query.
        
        **DELETE** /search?q=\ *chars*
            Delete documents which match query.
        """
        if cherrypy.request.method != 'DELETE':
            return WebSearcher.search(self, q, count, fields, multifields, sort, reverse)
        self.indexer.delete(q)
        cherrypy.response.status = httplib.ACCEPTED
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'PUT', 'POST'])
    def fields(self, name='', **params):
        """Return or store a field's parameters.
        
        **GET** /fields
            Return known field names.
            
            :return: [*string*,... ]
        
        **GET, PUT, POST** /fields/*chars*
            Set and return parameters for given field name.
            
            store=\ *chars*
            
            index=\ *chars*
            
            termvector=\ *chars*
            
            :return: {"store": *string*, "index": *string*, "termvector": *string*}
        
        **PUT** /fields/*chars*
            Set parameters for given field name.
        """
        if cherrypy.request.method in ('PUT', 'POST'):
            self.indexer.set(name, **params)
        if not name:
            return sorted(self.indexer.fields)
        with handleNotFound(KeyError):
            field = self.indexer.fields[name]
        return dict((name, str(getattr(field, name))) for name in ['store', 'index', 'termvector'])

def main(root, path='', config=None):
    "Attach root and run server."
    cherrypy.wsgiserver.WorkerThread = AttachedThread
    cherrypy.engine.subscribe('stop', root.indexer.close)
    cherrypy.config['engine.autoreload.on'] = False
    cherrypy.quickstart(root, path, config)

if __name__ == '__main__':
    import os, optparse
    parser = optparse.OptionParser(usage='python %prog [index_directory]')
    parser.add_option('-r', '--read-only', action='store_true', dest='read', help='expose only GET methods; no write lock')
    parser.add_option('-c', '--config', dest='config', help='optional configuration file or global json dict')
    options, args = parser.parse_args()
    if lucene.getVMEnv() is None:
        lucene.initVM(lucene.CLASSPATH, vmargs='-Xrs')
    root = (WebSearcher if options.read else WebIndexer)(*args)
    if options.config and not os.path.exists(options.config):
        options.config = {'global': json.loads(options.config)}
    main(root, config=options.config)
