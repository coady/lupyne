"""
Restful json `CherryPy <http://cherrypy.org/>`_ server.

    $ python server.py `directory`

CherryPy and Lucene VM integration issues:
 * Autoreload is not compatible with the VM initialization.
 * WorkerThreads must be attached to the VM.
 * Also recommended that the VM ignores keyboard interrupts for clean server shutdown.
"""

try:    # optimization
    import simplejson as json
except ImportError:
    import json
import threading
from contextlib import contextmanager
import lucene
import cherrypy
from cherrypy.wsgiserver import WorkerThread
from engine import Indexer

def json_tool():
    "Transform responses into json format."
    response = cherrypy.response
    if response.status is None and cherrypy.request.path_info != '/favicon.ico':
        response.headers['content-type'] = 'text/plain'
        response.body = json.dumps(response.body)
cherrypy.tools.json = cherrypy.Tool('before_finalize', json_tool)

class AttachedThread(WorkerThread):
    "Attach cherrypy threads to lucene VM."
    def run(self):
        lucene.getVMEnv().attachCurrentThread()
        WorkerThread.run(self)

@contextmanager
def handle404(exc):
    "Translate given exception into 404 Not Found."
    try:
        yield
    except exc:
        raise cherrypy.NotFound(cherrypy.request.path_info)

class Root(object):
    "Dispatch root with a delegated Indexer."
    _cp_config = {'tools.json.on': True, 'tools.gzip.on': True}
    def __init__(self, *args, **kwargs):
        self.indexer = Indexer(*args, **kwargs)
        self.lock = threading.Lock()
    @cherrypy.expose
    def index(self):
        """Return index information.
        
        **GET** /
            Return a mapping of the directory to the document count.
            
            :return: {*string*: *int*}
        """
        return {str(self.indexer.directory): len(self.indexer)}
    @cherrypy.expose
    def commit(self):
        """Commit write operations.
        
        **POST** /commit
        """
        with self.lock:
            self.indexer.commit()
    @cherrypy.expose
    def docs(self, id=None, docs='[]', fields='', multifields=''):
        """Return and index documents.
        
        **GET** /docs
            Return list of doc ids.
            
            :return: [*int*,... ]
        
        **GET** /docs/*int*?
            Return document mappings, optionally selected unique or multi-valued fields.
            
            &fields=\ *chars*,...
            
            &multifields=\ *chars*,...
            
            :return: {*string*: *string*\|\ *array*,... }
        
        **POST** /docs
            Add documents to index.
            
            docs=[{*string*: *string*\|\ *array*,... },... ]
        """
        fields = dict.fromkeys(filter(None, fields.split(',')))
        multifields = filter(None, multifields.split(','))
        if cherrypy.request.method == 'GET':
            if id is None:
                return list(self.indexer)
            with handle404(KeyError):
                doc = self.indexer[int(id)]
            return doc.dict(*multifields, **fields)
        for doc in json.loads(docs):
            self.indexer.add(doc)
    @cherrypy.expose
    def search(self, q, count=None, fields='', multifields='', sort=None, reverse='false'):
        """Run or delete a query.
        
        **DELETE** /search?q=\ *chars*
            Delete documents which match query.
        
        **GET** /search?q=\ *chars*,
            Return list document mappings and total doc count.
            
            &count=\ *int*
            
            &fields=\ *chars*,...
            
            &multifields=\ *chars*,...
            
            &sort=\ *chars*,...
            
            &reverse=true|false,
            
            :return: {"count": *int*, "docs": [{"__id__": *int*, "__score__": *number*, *string*: *string*\|\ *array*,... },... ]}
        """
        if cherrypy.request.method == 'DELETE':
            return self.indexer.delete(q)
        if count is not None:
            count = int(count)
        fields = dict.fromkeys(filter(None, fields.split(',')))
        multifields = filter(None, multifields.split(','))
        if sort is not None and ',' in sort:
            sort = fields.split(',')
        hits = self.indexer.search(q, count=count, sort=sort, reverse=json.loads(reverse))
        docs = [hit.dict(*multifields, **fields) for hit in hits]
        return {'count': hits.count, 'docs': docs}
    @cherrypy.expose
    def fields(self, name='', **params):
        """Return and store a field's parameters.
        
        **GET** /fields
            Return known field names.
            
            :return: [*string*,... ]
        
        **GET** /fields/*chars*
            Return parameters for given field name.
            
            :return: {"store": *string*, "index": *string*, "termvector": *string*}
        
        **PUT** /fields/*chars*
            Set parameters for given field name.
            
            store=\ *chars*
            
            index=\ *chars*
            
            termvector=\ *chars*
        """
        if not name:
            return sorted(self.indexer.fields)
        if cherrypy.request.method in ('PUT', 'POST'):
            self.indexer.set(name, **params)
        with handle404(KeyError):
            field = self.indexer.fields[name]
        return dict((name, str(getattr(field, name))) for name in ['store', 'index', 'termvector'])
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
        
        **GET** /terms/*chars*/[*chars*:*chars*]
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
        raise cherrypy.NotFound('/'.join(args))

def main(root, path='', config=None):
    "Attach root and run server."
    cherrypy.wsgiserver.WorkerThread = AttachedThread
    cherrypy.engine.subscribe('stop', root.indexer.close)
    cherrypy.config['engine.autoreload.on'] = False
    cherrypy.quickstart(root, path, config)

if __name__ == '__main__':
    import sys
    if lucene.getVMEnv() is None:
        lucene.initVM(lucene.CLASSPATH, vmargs='-Xrs')
    main(Root(*sys.argv[1:]), config={'global': {'server.socket_host': '0.0.0.0'}})
