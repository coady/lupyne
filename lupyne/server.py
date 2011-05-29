"""
Restful json `CherryPy <http://cherrypy.org/>`_ server.

The server script mounts a `WebSearcher`_ (read_only) or `WebIndexer`_ root.
Standard `CherryPy configuration <http://www.cherrypy.org/wiki/ConfigAPI>`_ applies,
and the provided `custom tools <#tools>`_ are also configurable.
All request and response bodies are `application/json values <http://tools.ietf.org/html/rfc4627.html#section-2.1>`_.

WebSearcher exposes resources for an IndexSearcher.
In addition to search requests, it provides access to term and document information in the index.
Note Lucene doc ids are ephemeral;  they should only be used across requests for the same index version.

 * :meth:`/ <WebSearcher.index>`
 * :meth:`/search <WebSearcher.search>`
 * :meth:`/docs <WebSearcher.docs>`
 * :meth:`/terms <WebSearcher.terms>`
 * :meth:`/update <WebSearcher.update>`

WebIndexer extends WebSearcher, exposing additional resources and methods for an Indexer.
Single documents may be added, deleted, or replaced by a unique indexed field.
Multiples documents may also be added or deleted by query at once.
By default changes are not visible until the update resource is called to commit a new index version.
If a near real-time Indexer is used (an experimental feature in Lucene), then changes are instantly searchable.
In such cases a commit still hasn't occurred;  the index based :meth:`validation headers <validate>` shouldn't be used for caching.

 * :meth:`/ <WebIndexer.index>`
 * :meth:`/search <WebIndexer.search>`
 * :meth:`/docs <WebIndexer.docs>`
 * :meth:`/fields <WebIndexer.fields>`
 * :meth:`/update <WebIndexer.update>`

Custom servers should create and mount WebSearchers and WebIndexers as needed.
:meth:`Caches <WebSearcher.update>` and :meth:`field settings <WebIndexer.fields>` can then be applied directly before `starting <#start>`_ the server.
WebSearchers and WebIndexers can of course also be subclassed for custom interfaces.

CherryPy and Lucene VM integration issues:
 * Monitors (such as autoreload) are not compatible with the VM unless threads are attached.
 * WorkerThreads must be also attached to the VM.
 * VM initialization must occur after daemonizing.
 * Recommended that the VM ignores keyboard interrupts (-Xrs) for clean server shutdown.
"""

from future_builtins import filter, map
import re
import time
import httplib
import heapq
import collections
import itertools, operator
import os, optparse
from email.utils import formatdate
from contextlib import contextmanager
try:
    import simplejson as json
except ImportError:
    import json
import lucene
import cherrypy
import engine

def tool(hook):
    "Return decorator to register tool at given hook point."
    def decorator(func):
        setattr(cherrypy.tools, func.__name__.rstrip('_'), cherrypy.Tool(hook, func))
        return func
    return decorator

@tool('before_handler')
def json_(indent=None, content_type='application/json', process_body=None):
    """Handle request bodies and responses in json format.
    
    :param indent: indentation level for pretty printing
    :param content_type: request media type and response content-type header
    :param process_body: optional function to process body into request.params
    """
    request = cherrypy.serving.request
    media_type = request.headers.get('content-type')
    if media_type == content_type:
        with HTTPError(httplib.BAD_REQUEST, ValueError, AttributeError):
            request.json = json.load(request.body)
        if process_body is not None:
            with HTTPError(httplib.BAD_REQUEST, TypeError):
                request.params.update(process_body(request.json))
    elif media_type is not None:
        message = "Received Content-Type header {0}; only {1} is supported.".format(media_type, content_type)
        raise cherrypy.HTTPError(httplib.UNSUPPORTED_MEDIA_TYPE, message)
    headers = cherrypy.response.headers
    handler = request.handler
    def json_handler(*args, **kwargs):
        body = handler(*args, **kwargs)
        if headers['content-type'].startswith('text/'):
            headers['content-type'] = content_type
            body = json.dumps(body, indent=indent)
        return body
    request.handler = json_handler

@tool('on_start_resource')
def allow(methods=('GET', 'HEAD')):
    "Only allow specified methods."
    request = cherrypy.serving.request
    if request.method not in methods and not isinstance(request.handler, cherrypy.HTTPError):
        cherrypy.response.headers['allow'] = ', '.join(methods)
        message = "The path {0!r} does not allow {1}.".format(request.path_info, request.method)
        raise cherrypy.HTTPError(httplib.METHOD_NOT_ALLOWED, message)

@tool('before_finalize')
def time_():
    "Return response time in headers."
    response = cherrypy.serving.response
    response.headers['x-response-time'] = time.time() - response.time

@tool('on_start_resource')
def validate(methods=('GET', 'HEAD'), etag=True, last_modified=True, max_age=None, expires=None):
    """Return and validate caching headers for GET requests.
    
    :param methods: only set headers for specified methods
    :param etag: return weak entity tag header based on index version and validate if-match headers
    :param last_modified: return last-modified header based on index timestamp and validate if-modified headers
    :param max_age: return cache-control max-age and age headers based on last update timestamp
    :param expires: return expires header offset from last update timestamp
    """
    request = cherrypy.serving.request
    headers = cherrypy.response.headers
    if request.method in methods and not isinstance(request.handler, cherrypy.HTTPError):
        if etag:
            headers['etag'] = 'W/"{0}"'.format(request.app.root.searcher.version)
            cherrypy.lib.cptools.validate_etags()
        if last_modified:
            headers['last-modified'] = formatdate(request.app.root.searcher.timestamp, usegmt=True)
            cherrypy.lib.cptools.validate_since()
        if max_age is not None:
            headers['age'] = int(time.time() - request.app.root.updated)
            headers['cache-control'] = 'max-age={0}'.format(max_age)
        if expires is not None:
            headers['expires'] = formatdate(expires + request.app.root.updated, usegmt=True)

@tool('before_handler')
def params(**types):
    "Convert specified request params."
    params = cherrypy.request.params
    with HTTPError(httplib.BAD_REQUEST, ValueError):
        for key in set(types).intersection(params):
            params[key] = types[key](params[key])

def json_error(version, **body):
    "Transform errors into json format."
    tool = cherrypy.request.toolmaps['tools'].get('json', {})
    cherrypy.response.headers['content-type'] = tool.get('content_type', 'application/json')
    return json.dumps(body, indent=tool.get('indent'))

def attach_thread(id=None):
    "Attach current cherrypy worker thread to lucene VM."
    lucene.getVMEnv().attachCurrentThread()

class Autoreloader(cherrypy.process.plugins.Autoreloader):
    "Autoreload monitor compatible with lucene VM."
    def run(self):
        attach_thread()
        cherrypy.process.plugins.Autoreloader.run(self)

class AttachedMonitor(cherrypy.process.plugins.Monitor):
    "Periodically run a callback function in an attached thread."
    def __init__(self, bus, callback, frequency=cherrypy.process.plugins.Monitor.frequency):
        def run():
            attach_thread()
            callback()
        cherrypy.process.plugins.Monitor.__init__(self, bus, run, frequency)

@contextmanager
def HTTPError(status, *exceptions):
    "Interpret exceptions as an HTTPError with given status code."
    try:
        yield
    except exceptions as exc:
        raise cherrypy.HTTPError(status, str(exc))

class WebSearcher(object):
    "Dispatch root with a delegated Searcher."
    _cp_config = dict.fromkeys(map('tools.{0}.on'.format, ['gzip', 'accept', 'json', 'allow', 'time', 'validate']), True)
    _cp_config.update({'error_page.default': json_error, 'tools.gzip.mime_types': ['text/html', 'text/plain', 'application/json'], 'tools.accept.media': 'application/json'})
    def __init__(self, *directories, **kwargs):
        self.searcher = engine.MultiSearcher(directories, **kwargs) if len(directories) > 1 else engine.IndexSearcher(*directories, **kwargs)
        self.updated = time.time()
    @classmethod
    def new(cls, *args, **kwargs):
        "Return new uninitialized root which can be mounted on dispatch tree before VM initialization."
        self = object.__new__(cls)
        self.args, self.kwargs = args, kwargs
        return self
    def init(self, vmargs='-Xrs', **kwargs):
        "Callback to initialize VM and root object after daemonizing."
        lucene.initVM(vmargs=vmargs, **kwargs)
        self.__init__(*self.__dict__.pop('args'), **self.__dict__.pop('kwargs'))
    def close(self):
        self.searcher.close()
    @staticmethod
    def parse(searcher, q, **options):
        "Return parsed query using q.* parser options."
        options = dict((key.partition('.')[-1], options[key]) for key in options if key.startswith('q.'))
        field = options.pop('field', [])
        fields = [field] if isinstance(field, basestring) else field
        fields = [name.partition('^')[::2] for name in fields]
        if any(boost for name, boost in fields):
            field = dict((name, float(boost or 1.0)) for name, boost in fields)
        elif isinstance(field, basestring):
            (field, boost), = fields
        else:
            field = [name for name, boost in fields] or ''
        if 'type' in options:
            with HTTPError(httplib.BAD_REQUEST, AttributeError):
                return getattr(engine.Query, options['type'])(field, q)
        for key in set(options) - set(['op', 'version']):
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                options[key] = json.loads(options[key])
        if q is not None:
            with HTTPError(httplib.BAD_REQUEST, lucene.JavaError):
                return searcher.parse(q, field=field, **options)
    @staticmethod
    def select(fields=None, **options):
        "Return parsed field selectors: stored, multi-valued, and indexed."
        if fields is not None:
            fields = dict.fromkeys(filter(None, fields.split(',')))
        multi = list(filter(None, options.get('fields.multi', '').split(',')))
        indexed = [field.split(':') for field in options.get('fields.indexed', '').split(',') if field]
        return fields, multi, indexed
    @cherrypy.expose
    @cherrypy.tools.json(process_body=lambda body: dict.fromkeys(body, True))
    @cherrypy.tools.allow(methods=['POST'])
    def update(self, **caches):
        """Refresh index version.
        
        **POST** /update
            Reopen searcher, optionally reloading caches, and return document count.
            
            ["filters"|"sorters"|"spellcheckers",... ]
            
            :return: *int*
        """
        self.searcher = self.searcher.reopen(**caches)
        self.updated = time.time()
        return len(self.searcher)
    @cherrypy.expose
    def index(self):
        """Return index information.
        
        **GET** /
            Return a mapping of the directory to the document count.
            
            :return: {*string*: *int*,... }
        """
        reader = self.searcher.indexReader
        readers = reader.sequentialSubReaders if lucene.MultiReader.instance_(reader) else [reader]
        return dict((unicode(reader.directory()), reader.numDocs()) for reader in readers)
    @cherrypy.expose
    def docs(self, *path, **options):
        """Return ids or documents.
        
        **GET** /docs
            Return list of doc ids.
            
            :return: [*int*,... ]
        
        **GET** /docs/[*int*\|\ *chars*/*chars*]?
            Return document mapping from id or unique name and value.
            Optionally select stored, multi-valued, and cached indexed fields.
            
            &fields=\ *chars*,... &fields.multi=\ *chars*,... &fields.indexed=\ *chars*\ [:*chars*],...
            
            :return: {*string*: *string*\|\ *array*,... }
        """
        searcher = self.searcher
        if not path:
            return list(searcher)
        fields, multi, indexed = self.select(**options)
        with HTTPError(httplib.NOT_FOUND, ValueError):
            id, = map(int, path) if len(path) == 1 else searcher.docs(*path)
        with HTTPError(httplib.NOT_FOUND, lucene.JavaError):
            doc = searcher[id] if fields is None else searcher.get(id, *itertools.chain(fields, multi))
        result = doc.dict(*multi, **(fields or {}))
        result.update((item[0], searcher.comparator(*item)[id]) for item in indexed)
        return result
    @cherrypy.expose
    @cherrypy.tools.params(count=int, start=int, mlt=int, spellcheck=int, timeout=float,
        **{'facets.count': int, 'facets.min': int, 'group.count': int, 'group.limit': int, 'hl.count': int})
    def search(self, q=None, count=None, start=0, fields=None, sort=None, facets='', group='', hl='', mlt=None, spellcheck=0, timeout=None, **options):
        """Run query and return documents.
        
        **GET** /search?
            Return list of document objects and total doc count.
            
            &q=\ *chars*\ &q.type=[term|prefix|wildcard]&q.\ *chars*\ =...,
                query, optional type to skip parsing, and optional parser settings: q.field, q.op,...
            
            &filter=\ *chars*
                | cached filter applied to the query
                | if a previously cached filter is not found, the value will be parsed as a query
            
            &count=\ *int*\ &start=0
                maximum number of docs to return and offset to start at
            
            &fields=\ *chars*,... &fields.multi=\ *chars*,... &fields.indexed=\ *chars*\ [:*chars*],...
                only include selected stored fields; multi-valued fields returned in an array; indexed fields with optional type are cached
            
            &sort=\ [-]\ *chars*\ [:*chars*],... &sort.scores[=max]
                | field name, optional type, minus sign indicates descending
                | optionally score docs, additionally compute maximum score
            
            &facets=\ *chars*,... &facets.count=\ *int*\&facets.min=0
                | include facet counts for given field names; facets filters are cached
                | optional maximum number of most populated facet values per field, and minimum count to return
            
            &group=\ *chars*\ [:*chars*]&group.count=1&group.limit=\ *int*
                | group documents by field value with optional type, up to given maximum count
                | limit number of groups which return docs
            
            &hl=\ *chars*,... &hl.count=1&hl.tag=strong&hl.enable=[fields|terms]
                | stored fields to return highlighted
                | optional maximum fragment count and html tag name
                | optionally enable matching any field or any term
            
            &mlt=\ *int*\ &mlt.fields=\ *chars*,... &mlt.\ *chars*\ =...,
                | doc index (or id without a query) to find MoreLikeThis
                | optional document fields to match
                | optional MoreLikeThis settings: mlt.minTermFreq, mlt.minDocFreq,...
            
            &spellcheck=\ *int*
                | maximum number of spelling corrections to return for each query term, grouped by field
                | original query is still run; use q.spellcheck=true to affect query parsing
            
            &timeout=\ *number*
                timeout search after elapsed number of seconds
            
            :return:
                | {
                | "query": *string*,
                | "count": *int*\|null,
                | "maxscore": *number*\|null,
                | "docs": [{"__id__": *int*, "__score__": *number*, "__highlights__": {*string*: *array*,... }, *string*: *string*\|\ *array*,... },... ],
                | "facets": {*string*: {*string*: *int*,... },... },
                | "groups": [{"count": *int*, "value": *value*, "docs": [{... },... ]},... ]
                | "spellcheck": {*string*: {*string*: [*string*,... ],... },... },
                | }
        """
        searcher = self.searcher
        reverse = False
        if sort is not None:
            sort = (re.match('(-?)(\w+):?(\w*)', field).groups() for field in sort.split(','))
            sort = [(name, (type or 'string'), (reverse == '-')) for reverse, name, type in sort]
            if count is None:
                with HTTPError(httplib.BAD_REQUEST, ValueError, AttributeError):
                    reverse, = set(reverse for name, type, reverse in sort) # only one sort direction allowed with unlimited count
                    comparators = [searcher.comparator(name, type) for name, type, reverse in sort]
                sort = comparators[0].__getitem__ if len(comparators) == 1 else lambda id: tuple(map(operator.itemgetter(id), comparators))
            else:
                with HTTPError(httplib.BAD_REQUEST, AttributeError):
                    sort = [searcher.sorter(name, type, reverse=reverse) for name, type, reverse in sort]
        q = self.parse(searcher, q, **options)
        qfilter = options.pop('filter', None)
        if qfilter is not None and qfilter not in searcher.filters:
            searcher.filters[qfilter] = engine.Query.__dict__['filter'](self.parse(searcher, qfilter, **options))
        qfilter = searcher.filters.get(qfilter)
        if mlt is not None:
            if q is not None:
                mlt = searcher.search(q, count=mlt+1, sort=sort, reverse=reverse).ids[mlt]
            mltfields = filter(None, options.pop('mlt.fields', '').split(','))
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                attrs = dict((key.partition('.')[-1], json.loads(options[key])) for key in options if key.startswith('mlt.'))
            q = searcher.morelikethis(mlt, *mltfields, **attrs)
        if count is not None:
            count += start
        if count == 0:
            start = count = 1
        scores = options.get('sort.scores')
        scores = {'scores': scores is not None, 'maxscore': scores == 'max'}
        hits = searcher.search(q, filter=qfilter, count=count, sort=sort, reverse=reverse, timeout=timeout, **scores)[start:]
        result = {'query': q and unicode(q), 'count': hits.count, 'maxscore': hits.maxscore}
        tag, enable = options.get('hl.tag', 'strong'), options.get('hl.enable', '')
        hlcount = options.get('hl.count', 1)
        if hl:
            hl = dict((name, searcher.highlighter(q, name, terms='terms' in enable, fields='fields' in enable, tag=tag)) for name in hl.split(','))
        fields, multi, indexed = self.select(fields, **options)
        if fields is None:
            fields = {}
        else:
            hits.fields = lucene.MapFieldSelector(list(itertools.chain(fields, multi)))
        indexed = dict((item[0], searcher.comparator(*item)) for item in indexed)
        docs = []
        groups = collections.defaultdict(lambda: {'docs': [], 'count': 0, 'index': len(groups)})
        gcount = options.get('group.count', 1)
        glimit = options.get('group.limit', float('inf'))
        if group:
            with HTTPError(httplib.BAD_REQUEST, AttributeError):
                group = searcher.comparator(*group.split(':'))
            ids, scores = [], []
            for id, score in hits.items():
                item = groups[group[id]]
                item['count'] += 1
                if item['count'] <= gcount and item['index'] < glimit:
                    ids.append(id)
                    scores.append(score)
            hits.ids, hits.scores = ids, scores
        for hit in hits:
            doc = hit.dict(*multi, **fields)
            doc.update((name, indexed[name][hit.id]) for name in indexed)
            fragments = (hl[name].fragments(hit.id, hlcount) for name in hl)
            if hl:
                doc['__highlights__'] = dict((name, value) for name, value in zip(hl, fragments) if value is not None)
            (groups[group[hit.id]]['docs'] if group else docs).append(doc)
        for name in groups:
            groups[name]['value'] = name
        if group:
            result['groups'] = sorted(groups.values(), key=lambda item: item.pop('index'))
        else:
            result['docs'] = docs
        q = q or lucene.MatchAllDocsQuery()
        if facets:
            facets = (tuple(facet.split(':')) if ':' in facet else facet for facet in facets.split(','))
            facets = result['facets'] = searcher.facets(q, *facets)
            if 'facets.min' in options:
                for name, counts in facets.items():
                    facets[name] = dict((term, count) for term, count in counts.items() if count >= options['facets.min'])
            if 'facets.count' in options:
                for name, counts in facets.items():
                    facets[name] = dict((term, counts[term]) for term in heapq.nlargest(options['facets.count'], counts, key=counts.__getitem__))
        if spellcheck:
            terms = result['spellcheck'] = collections.defaultdict(dict)
            for name, value in engine.Query.__dict__['terms'](q):
                terms[name][value] = list(itertools.islice(searcher.correct(name, value), spellcheck))
        return result
    @cherrypy.expose
    @cherrypy.tools.params(count=int, step=int)
    def terms(self, name='', value=':', *path, **options):
        """Return data about indexed terms.
        
        **GET** /terms?
            Return field names, with optional selection.
            
            &option=\ *chars*
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*\[:int|float\]?step=0
            Return term values for given field name, with optional type and step for numeric encoded values.
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*/*chars*\[\*\|?\|:*chars*\|~\ *number*\]
            Return term values (wildcards, slices, or fuzzy terms) for given field name.
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*/*chars*\[\*\|~\]?count=\ *int*
            Return spellchecked term values ordered by decreasing document frequency.
            Prefixes (*) are optimized to be suitable for real-time query suggestions; all terms are cached.
            
            :return: [*string*,... ]
        
        **GET** /terms/*chars*/*chars*
            Return document count for given term.
            
            :return: *int*
        
        **GET** /terms/*chars*/*chars*/docs
            Return document ids for given term.
            
            :return: [*int*,... ]
        
        **GET** /terms/*chars*/*chars*/docs/counts
            Return document ids and frequency counts for given term.
            
            :return: [[*int*, *int*],... ]
        
        **GET** /terms/*chars*/*chars*/docs/positions
            Return document ids and positions for given term.
            
            :return: [[*int*, [*int*,... ]],... ]
        """
        searcher = self.searcher
        if not name:
            return sorted(searcher.names(**options))
        if ':' in name:
            with HTTPError(httplib.BAD_REQUEST, ValueError, AttributeError):
                name, type = name.split(':')
                type = getattr(__builtins__, type)
            return list(searcher.numbers(name, step=options.get('step', 0), type=type))
        if ':' in value:
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                start, stop = value.split(':')
            return list(searcher.terms(name, start, stop or None))
        if 'count' in options:
            if value.endswith('*'):
                return searcher.suggest(name, value.rstrip('*'), options['count'])
            if value.endswith('~'):
                return list(itertools.islice(searcher.correct(name, value.rstrip('~')), options['count']))
        if '*' in value or '?' in value:
            return list(searcher.terms(name, value))
        if '~' in value:
            with HTTPError(httplib.BAD_REQUEST, ValueError):
                value, similarity = value.split('~')
                similarity = float(similarity or 0.5)
            return list(searcher.terms(name, value, minSimilarity=similarity))
        if not path:
            return searcher.count(name, value)
        if path[0] == 'docs':
            if path[1:] == ():
                return list(searcher.docs(name, value))
            if path[1:] == ('counts',):
                return list(searcher.docs(name, value, counts=True))
            if path[1:] == ('positions',):
                return list(searcher.positions(name, value))
        raise cherrypy.NotFound()

class WebIndexer(WebSearcher):
    "Dispatch root with a delegated Indexer, exposing write methods."
    def __init__(self, *args, **kwargs):
        self.indexer = engine.Indexer(*args, **kwargs)
        self.updated = time.time()
    @property
    def searcher(self):
        return self.indexer.indexSearcher
    def close(self):
        self.indexer.close()
        WebSearcher.close(self)
    def refresh(self):
        if self.indexer.nrt:
            self.indexer.refresh()
            self.updated = time.time()
        else:
            cherrypy.response.status = httplib.ACCEPTED
    @cherrypy.expose
    @cherrypy.tools.json(process_body=lambda body: {'directories': list(body)})
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'POST'])
    def index(self, directories=()):
        """Add indexes.  See :meth:`WebSearcher.index` for GET method.
        
        **POST** /
            Add indexes without optimization.
            
            [*string*,... ]
        """
        if cherrypy.request.method == 'POST':
            for directory in directories:
                self.indexer += directory
            self.refresh()
        return {unicode(self.indexer.directory): len(self.indexer)}
    @cherrypy.expose
    def update(self, **options):
        """Commit index changes and refresh index version.
        
        **POST** /update
            Commit write operations and return document count.  See :meth:`WebSearcher.update` for caching options.
            
            ["expunge"|"optimize",... ]
            
            :return: *int*
        """
        self.indexer.commit(**options)
        self.updated = time.time()
        return len(self.indexer)
    update._cp_config = dict(WebSearcher.update._cp_config)
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'POST', 'PUT', 'DELETE'])
    def docs(self, *path, **options):
        """Add or return documents.  See :meth:`WebSearcher.docs` for GET method.
        
        **POST** /docs
            Add documents to index.
            
            [{*string*: *string*\|\ *array*,... },... ]
        
        **PUT, DELETE** /docs/*chars*/*chars*
            Set or delete document.  Unique term should be indexed and is added to the new document.
            
            {*string*: *string*\|\ *array*,... }
        """
        with HTTPError(httplib.NOT_FOUND, IndexError):
            allow([('GET', 'HEAD', 'POST'), ('GET', 'HEAD'), ('GET', 'HEAD', 'PUT', 'DELETE')][len(path)])
        request = cherrypy.serving.request
        if request.method in ('GET', 'HEAD'):
            return WebSearcher.docs(self, *path, **options)
        if request.method == 'DELETE':
            self.indexer.delete(*path)
        elif request.method == 'PUT':
            name, value = path
            doc = getattr(request, 'json', {})
            with HTTPError(httplib.CONFLICT, KeyError, AssertionError):
                assert self.indexer.fields[name].index.indexed, 'unique field must be indexed'
            with HTTPError(httplib.BAD_REQUEST, AssertionError):
                assert doc.setdefault(name, value) == value, 'multiple values for unique field'
            self.indexer.update(name, value, doc)
        else:
            for doc in getattr(request, 'json', ()):
                self.indexer.add(doc)
        self.refresh()
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'DELETE'])
    def search(self, q=None, **options):
        """Run or delete a query.  See :meth:`WebSearcher.search` for GET method.
        
        **DELETE** /search?q=\ *chars*
            Delete documents which match query.
        """
        if cherrypy.request.method != 'DELETE':
            return WebSearcher.search(self, q, **options)
        if q is None:
            self.indexer.deleteAll()
        else:
            self.indexer.delete(self.parse(self.searcher, q, **options))
        self.refresh()
    search._cp_config.update(WebSearcher.search._cp_config)
    @cherrypy.expose
    @cherrypy.tools.json(process_body=dict)
    @cherrypy.tools.allow(methods=['GET', 'HEAD', 'PUT'])
    @cherrypy.tools.validate(on=False)
    def fields(self, name='', **settings):
        """Return or store a field's parameters.
        
        **GET** /fields
            Return known field names.
            
            :return: [*string*,... ]
        
        **GET, PUT** /fields/*chars*
            Set and return parameters for given field name.
            
            {"store"|"index"|"termvector": *string*\|true|false,... }
            
            :return: {"store": *string*, "index": *string*, "termvector": *string*}
        """
        if not name:
            allow()
            return sorted(self.indexer.fields)
        if cherrypy.request.method == 'PUT':
            if name not in self.indexer.fields:
                cherrypy.response.status = httplib.CREATED
            with HTTPError(httplib.BAD_REQUEST, AttributeError):
                self.indexer.set(name, **settings)
        with HTTPError(httplib.NOT_FOUND, KeyError):
            field = self.indexer.fields[name]
        return dict((name, str(getattr(field, name))) for name in ['store', 'index', 'termvector'])

def start(root=None, path='', config=None, pidfile='', daemonize=False, autoreload=0, autoupdate=0, callback=None):
    """Attach root, subscribe to plugins, and start server.
    
    :param root,path,config: see cherrypy.quickstart
    :param pidfile,daemonize,autoreload,autoupdate: see command-line options
    :param callback: optional callback function scheduled after daemonizing
    """
    cherrypy.engine.subscribe('start_thread', attach_thread)
    if hasattr(root, 'close'):
        cherrypy.engine.subscribe('stop', root.close)
    cherrypy.config['engine.autoreload.on'] = False
    if pidfile:
        cherrypy.process.plugins.PIDFile(cherrypy.engine, os.path.abspath(pidfile)).subscribe()
    if daemonize:
        cherrypy.config['log.screen'] = False
        cherrypy.process.plugins.Daemonizer(cherrypy.engine).subscribe()
    if autoreload:
        Autoreloader(cherrypy.engine, autoreload).subscribe()
    if autoupdate:
        AttachedMonitor(cherrypy.engine, root.update, autoupdate).subscribe()
    if callback:
        priority = (cherrypy.process.plugins.Daemonizer.start.priority + cherrypy.server.start.priority) // 2
        cherrypy.engine.subscribe('start', callback, priority)
    cherrypy.quickstart(root, path, config)

parser = optparse.OptionParser(usage='python %prog [index_directory ...]')
parser.add_option('-r', '--read-only', action='store_true', help='expose only read methods; no write lock')
parser.add_option('-c', '--config', help='optional configuration file or json object of global params')
parser.add_option('-p', '--pidfile', metavar='FILE', help='store the process id in the given file')
parser.add_option('-d', '--daemonize', action='store_true', help='run the server as a daemon')
parser.add_option('--autoreload', type=int, metavar='SECONDS', help='automatically reload modules; replacement for engine.autoreload')
parser.add_option('--autoupdate', type=int, metavar='SECONDS', help='automatically update index version')

if __name__ == '__main__':
    options, args = parser.parse_args()
    read_only = options.__dict__.pop('read_only')
    if options.config and not os.path.exists(options.config):
        options.config = {'global': json.loads(options.config)}
    cls = WebSearcher if (read_only or len(args) > 1) else WebIndexer
    root = cls.new(*map(os.path.abspath, args))
    start(root, callback=root.init, **options.__dict__)
