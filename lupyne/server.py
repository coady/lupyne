"""
Restful json `CherryPy <http://cherrypy.org/>`_ server.

    $ python server.py `directory`,... [-r|--read-only] [-c|--config filename]

CherryPy and Lucene VM integration issues:
 * Autoreload is not compatible with the VM initialization.
 * WorkerThreads must be attached to the VM.
 * Also recommended that the VM ignores keyboard interrupts for clean server shutdown.
"""

try:    # optimization
    import simplejson as json
except ImportError:
    import json
import re
import httplib
import threading
from contextlib import contextmanager
import lucene
import cherrypy
from cherrypy.wsgiserver import WorkerThread
import engine

def json_tool():
    "Transform responses into json format."
    response = cherrypy.response
    if response.status is None and response.headers['content-type'].startswith('text/'):
        response.headers['content-type'] = 'text/x-json'
        response.body = json.dumps(response.body)
cherrypy.tools.json = cherrypy.Tool('before_finalize', json_tool)

def allow_tool(methods=['GET', 'HEAD']):
    "Only allow specified methods."
    request = cherrypy.request
    if request.method not in methods:
        cherrypy.response.headers['allow'] = ', '.join(methods)
        message = "The path {0!r} does not allow {1}.".format(request.path_info, request.method)
        raise cherrypy.HTTPError(httplib.METHOD_NOT_ALLOWED, message)
cherrypy.tools.allow = cherrypy.Tool('on_start_resource', allow_tool)

def json_error(version, **body):
    "Transform errors into json format."
    cherrypy.response.headers['content-type'] = 'text/x-json'
    return json.dumps(body)

class AttachedThread(WorkerThread):
    "Attach cherrypy threads to lucene VM."
    def run(self):
        lucene.getVMEnv().attachCurrentThread()
        WorkerThread.run(self)

@contextmanager
def HTTPError(status, *exceptions):
    "Interpret exceptions as an HTTPError with given status code."
    try:
        yield
    except exceptions as exc:
        raise cherrypy.HTTPError(status, str(exc))

class WebSearcher(object):
    "Dispatch root with a delegated Searcher."
    _cp_config = {'tools.json.on': True, 'tools.allow.on': True, 'error_page.default': json_error,
        'tools.gzip.on': True, 'tools.gzip.mime_types': ['text/html', 'text/plain', 'text/x-json']}
    def __init__(self, *directories, **kwargs):
        self.indexer = engine.MultiSearcher(directories, **kwargs) if len(directories) > 1 else engine.IndexSearcher(*directories, **kwargs)
    def parse(self, q, **options):
        "Return parsed query using q.* parser options."
        options = dict((key[2:], options[key]) for key in options if key.startswith('q.'))
        field = options.pop('field', [])
        fields = [field] if isinstance(field, basestring) else field
        fields = [re.match('(\w+)\^?([\d\.]*)', name).groups() for name in fields]
        if any(boost for name, boost in fields):
            field = dict((name, float(boost or 1.0)) for name, boost in fields)
        elif isinstance(field, basestring):
            (field, boost), = fields
        else:
            field = [name for name, boost in fields]
        return q and self.indexer.parse(q, field, **options)
    @cherrypy.expose
    def index(self):
        """Return index information.
        
        **GET** /
            Return a mapping of the directory to the document count.
            
            :return: {*string*: *int*}
        """
        if not isinstance(self.indexer, lucene.MultiSearcher):
            return {str(self.indexer.directory): len(self.indexer)}
        if hasattr(lucene.MultiReader, 'sequentialSubReaders'):
            return dict((str(reader.directory()), reader.numDocs()) for reader in self.indexer.sequentialSubReaders)
        return {str(self.indexer): len(self.indexer)}
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
        with HTTPError(httplib.NOT_FOUND, ValueError, lucene.JavaError):
            doc = self.indexer[int(id)]
        fields = dict.fromkeys(filter(None, fields.split(',')))
        multifields = filter(None, multifields.split(','))
        return doc.dict(*multifields, **fields)
    @cherrypy.expose
    def search(self, q=None, count=None, fields='', multifields='', sort=None, facets='', hl='', **options):
        """Run query and return documents.
        
        **GET** /search?
            Return list of document objects and total doc count.
            
            &q=\ *chars*\ &q.\ *chars*\ =...,
                query and optional parser settings: q.field, q.op,...
            
            &count=\ *int*
                maximum number of docs to return
            
            &fields=\ *chars*,...
                only include selected fields
            
            &multifields=\ *chars*,...
                multi-valued fields returned in an array
            
            &sort=\ [-]\ *chars*\ [:*chars*],...
                field name, optional type, minus sign indicates descending
            
            &facets=\ *chars*,...
                include facet counts for given field names
            
            &hl=\ *chars*,... &hl.count=1&hl.tag=strong&hl.enable=[fields|terms]
                | stored fields to return highlighted
                | optional maximum fragment count and html tag name
                | optionally enable matching any field or any term
            
            :return:
                | {
                | "count": *int*,
                | "docs": [{"__id__": *int*, "__score__": *number*, "__highlights__": {*string*: *array*,... }, *string*: *string*\|\ *array*,... },... ],
                | "facets": {*chars*: {*chars*: *int*,... },... },
                | }
        """
        if count is not None:
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                count = int(count)
        fields = dict.fromkeys(filter(None, fields.split(',')))
        multifields = filter(None, multifields.split(','))
        reverse = False
        if sort is not None:
            sort = (re.match('(-?)(\w+):?(\w*)', field).groups() for field in sort.split(','))
            sort = [(name, (type.upper() or 'STRING'), (reverse == '-')) for reverse, name, type in sort]
            if count is None:
                with HTTPError(httplib.BAD_REQUEST, ValueError):
                    (name, type, reverse), = sort # only one sort field allowed with unlimited count
                sort = self.indexer.comparator(name, type).__getitem__
            else:
                with HTTPError(httplib.BAD_REQUEST, AttributeError):
                    sort = [lucene.SortField(name, getattr(lucene.SortField, type), reverse) for name, type, reverse in sort]
        q = self.parse(q, **options)
        hits = self.indexer.search(q, count=count, sort=sort, reverse=reverse)
        result = {'count': hits.count, 'docs': []}
        tag = options.get('hl.tag', 'strong')
        field = 'fields' not in options.get('hl.enable', '') or None
        span = 'terms' not in options.get('hl.enable', '')
        if hl:
            hl = dict((name, self.indexer.highlighter(q, span=span, field=(field and name), formatter=tag)) for name in hl.split(','))
        with HTTPError(httplib.BAD_REQUEST, ValueError):
            count = int(options.get('hl.count', 1))
        for hit in hits:
            doc = hit.dict(*multifields, **fields)
            result['docs'].append(doc)
            if hl:
                doc['__highlights__'] = dict((name, hl[name].fragments(hit[name], count)) for name in hl if name in hit)
        if facets:
            result['facets'] = self.indexer.facets(hits.ids, *facets.split(','))
        return result
    @cherrypy.expose
    def terms(self, name='', value=':', docs='', counts='', **options):
        """Return data about indexed terms.
        
        **GET** /terms?
            Return field names, with optional selection.
            
            &option=\ *chars*
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*
            Return term values for given field name.
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*/*chars*\[\*\|:*chars*\|~\ *float*\]
            Return term values (wildcards, slices, or fuzzy terms) for given field name.
            
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
        if '*' in value:
            return list(self.indexer.terms(name, value))
        if '~' in value:
            value, similarity = value.split('~')
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                similarity = float(similarity or 0.5)
            return list(self.indexer.terms(name, value, minSimilarity=similarity))
        if not docs:
            return self.indexer.count(name, value)
        if docs == 'docs':
            if counts == 'positions':
                return list(self.indexer.positions(name, value))
            if counts in ('', 'counts'):
                return list(self.indexer.docs(name, value, counts=bool(counts)))
        raise cherrypy.NotFound()

class WebIndexer(WebSearcher):
    "Dispatch root which extends searcher to include write methods."
    def __init__(self, *args, **kwargs):
        self.indexer = engine.Indexer(*args, **kwargs)
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
        """Add or return documents.  See :meth:`WebSearcher.docs` for GET method.
        
        **POST** /docs
            Add documents to index.
            
            docs=[{*string*: *string*\|\ *array*,... },... ]
        """
        if cherrypy.request.method != 'POST':
            return WebSearcher.docs(self, id, fields, multifields)
        with HTTPError(httplib.BAD_REQUEST, ValueError):
            docs = json.loads(docs)
        for doc in docs:
            self.indexer.add(doc)
        cherrypy.response.status = httplib.ACCEPTED
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'DELETE'])
    def search(self, q=None, **options):
        """Run or delete a query.  See :meth:`WebSearcher.search` for GET method.
        
        **DELETE** /search?q=\ *chars*
            Delete documents which match query.
        """
        if cherrypy.request.method != 'DELETE':
            return WebSearcher.search(self, q, **options)
        self.indexer.delete(self.parse(q, **options))
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
        """
        if cherrypy.request.method in ('PUT', 'POST'):
            self.indexer.set(name, **params)
        if not name:
            return sorted(self.indexer.fields)
        with HTTPError(httplib.NOT_FOUND, KeyError):
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
    parser = optparse.OptionParser(usage='python %prog [index_directory ...]')
    parser.add_option('-r', '--read-only', action='store_true', dest='read', help='expose only GET methods; no write lock')
    parser.add_option('-c', '--config', dest='config', help='optional configuration file or global json dict')
    options, args = parser.parse_args()
    if lucene.getVMEnv() is None:
        lucene.initVM(lucene.CLASSPATH, vmargs='-Xrs')
    root = (WebSearcher if (options.read or len(args) > 1) else WebIndexer)(*args)
    if options.config and not os.path.exists(options.config):
        options.config = {'global': json.loads(options.config)}
    main(root, config=options.config)
