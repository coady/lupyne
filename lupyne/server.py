"""
Restful json `CherryPy <http://cherrypy.org/>`_ server.

    $ python server.py `directory`,... [-r|--read-only] [-c|--config filename]

CherryPy and Lucene VM integration issues:
 * Autoreload is not compatible with the VM initialization.
 * WorkerThreads must be attached to the VM.
 * Also recommended that the VM ignores keyboard interrupts for clean server shutdown.
"""

import re
import httplib
import threading
from contextlib import contextmanager
try:
    import simplejson as json
except ImportError:
    import json
import lucene
import cherrypy
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
    if request.method not in methods and not isinstance(request.handler, cherrypy.HTTPError):
        cherrypy.response.headers['allow'] = ', '.join(methods)
        message = "The path {0!r} does not allow {1}.".format(request.path_info, request.method)
        raise cherrypy.HTTPError(httplib.METHOD_NOT_ALLOWED, message)
cherrypy.tools.allow = cherrypy.Tool('on_start_resource', allow_tool)

def json_error(version, **body):
    "Transform errors into json format."
    cherrypy.response.headers['content-type'] = 'text/x-json'
    return json.dumps(body)

def attach_thread(id):
    "Attach current cherrypy worker thread to lucene VM."
    lucene.getVMEnv().attachCurrentThread()

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
        options = dict((key.partition('.')[-1], options[key]) for key in options if key.startswith('q.'))
        field = options.pop('field', [])
        fields = [field] if isinstance(field, basestring) else field
        fields = [re.match('(\w+)\^?([\d\.]*)', name).groups() for name in fields]
        if any(boost for name, boost in fields):
            field = dict((name, float(boost or 1.0)) for name, boost in fields)
        elif isinstance(field, basestring):
            (field, boost), = fields
        else:
            field = [name for name, boost in fields] or ''
        for key in set(options) - set(['op', 'version']):
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                options[key] = json.loads(options[key])
        return q and self.indexer.parse(q, field, **options)
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    def refresh(self):
        """Reopen searcher.
        
        **POST** /refresh
        """
        self.indexer = self.indexer.reopen()
        return len(self.indexer)
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
    def search(self, q=None, count=None, start=0, fields='', multifields='', sort=None, facets='', hl='', mlt=None, **options):
        """Run query and return documents.
        
        **GET** /search?
            Return list of document objects and total doc count.
            
            &q=\ *chars*\ &q.\ *chars*\ =...,
                query and optional parser settings: q.field, q.op,...
            
            &count=\ *int*\ &start=0
                maximum number of docs to return and offset to start at
            
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
            
            &mlt=\ *int*\ &mlt.fields=\ *chars*,... &mlt.\ *chars*\ =...,
                | doc index (or id without a query) to find MoreLikeThis
                | optional document fields to match
                | optional MoreLikeThis settings: mlt.minTermFreq, mlt.minDocFreq,...
            
            :return:
                | {
                | "query": *string*,
                | "count": *int*,
                | "docs": [{"__id__": *int*, "__score__": *number*, "__highlights__": {*string*: *array*,... }, *string*: *string*\|\ *array*,... },... ],
                | "facets": {*chars*: {*chars*: *int*,... },... },
                | }
        """
        with HTTPError(httplib.BAD_REQUEST, ValueError):
            start = int(start)
        if count is not None:
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                count = int(count) + start
        fields = dict.fromkeys(filter(None, fields.split(',')))
        multifields = filter(None, multifields.split(','))
        reverse = False
        searcher = getattr(self.indexer, 'indexSearcher', self.indexer)
        if sort is not None:
            sort = (re.match('(-?)(\w+):?(\w*)', field).groups() for field in sort.split(','))
            sort = [(name, (type.upper() or 'STRING'), (reverse == '-')) for reverse, name, type in sort]
            if count is None:
                with HTTPError(httplib.BAD_REQUEST, ValueError):
                    (name, type, reverse), = sort # only one sort field allowed with unlimited count
                sort = searcher.comparator(name, type).__getitem__
            else:
                with HTTPError(httplib.BAD_REQUEST, AttributeError):
                    sort = [lucene.SortField(name, getattr(lucene.SortField, type), reverse) for name, type, reverse in sort]
        q = self.parse(q, **options)
        if mlt is not None:
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                mlt = int(mlt)
            if q is not None:
                mlt = searcher.search(q, count=mlt+1, sort=sort, reverse=reverse).ids[mlt]
            mltfields = filter(None, options.pop('mlt.fields', '').split(','))
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                attrs = dict((key.partition('.')[-1], json.loads(options[key])) for key in options if key.startswith('mlt.'))
            q = searcher.morelikethis(mlt, *mltfields, **attrs)
        hits = searcher.search(q, count=count, sort=sort, reverse=reverse)
        result = {'query': unicode(q), 'count': hits.count, 'docs': []}
        tag = options.get('hl.tag', 'strong')
        field = 'fields' not in options.get('hl.enable', '') or None
        span = 'terms' not in options.get('hl.enable', '')
        if hl:
            hl = dict((name, searcher.highlighter(q, span=span, field=(field and name), formatter=tag)) for name in hl.split(','))
        with HTTPError(httplib.BAD_REQUEST, ValueError):
            count = int(options.get('hl.count', 1))
        for hit in hits[start:]:
            doc = hit.dict(*multifields, **fields)
            result['docs'].append(doc)
            if hl:
                doc['__highlights__'] = dict((name, hl[name].fragments(hit[name], count)) for name in hl if name in hit)
        if facets:
            result['facets'] = searcher.facets(engine.Query.filter.im_func(q), *facets.split(','))
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
    def refresh(self):
        raise cherrypy.HTTPRedirect('/commit', httplib.MOVED_PERMANENTLY)
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
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'PUT'])
    def fields(self, name='', **params):
        """Return or store a field's parameters.
        
        **GET** /fields
            Return known field names.
            
            :return: [*string*,... ]
        
        **GET, PUT** /fields/*chars*
            Set and return parameters for given field name.
            
            store=\ *chars*
            
            index=\ *chars*
            
            termvector=\ *chars*
            
            :return: {"store": *string*, "index": *string*, "termvector": *string*}
        """
        if cherrypy.request.method == 'PUT':
            self.indexer.set(name, **params)
        if not name:
            return sorted(self.indexer.fields)
        with HTTPError(httplib.NOT_FOUND, KeyError):
            field = self.indexer.fields[name]
        return dict((name, str(getattr(field, name))) for name in ['store', 'index', 'termvector'])

def main(root, path='', config=None):
    "Attach root and run server."
    cherrypy.engine.subscribe('start_thread', attach_thread)
    cherrypy.engine.subscribe('stop', root.indexer.close)
    cherrypy.config['engine.autoreload.on'] = False
    cherrypy.quickstart(root, path, config)

if __name__ == '__main__':
    import os, optparse
    parser = optparse.OptionParser(usage='python %prog [index_directory ...]')
    parser.add_option('-r', '--read-only', action='store_true', help='expose only read methods; no write lock')
    parser.add_option('-c', '--config', help='optional configuration file or json object of global params')
    parser.add_option('-p', '--pidfile', help='store the process id in the given file')
    parser.add_option('-d', '--daemonize', action='store_true', help='run the server as a daemon')
    options, args = parser.parse_args()
    if lucene.getVMEnv() is None:
        lucene.initVM(lucene.CLASSPATH, vmargs='-Xrs')
    root = (WebSearcher if (options.read_only or len(args) > 1) else WebIndexer)(*args)
    if options.config and not os.path.exists(options.config):
        options.config = {'global': json.loads(options.config)}
    if options.pidfile:
        cherrypy.process.plugins.PIDFile(cherrypy.engine, os.path.abspath(options.pidfile)).subscribe()
    if options.daemonize:
        cherrypy.config['log.screen'] = False
        cherrypy.process.plugins.Daemonizer(cherrypy.engine).subscribe()
    main(root, config=options.config)
