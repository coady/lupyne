"""
Restful json `CherryPy <http://cherrypy.org/>`_ server.

The server script mounts a `WebSearcher`_ (read_only) or `WebIndexer`_ root.
Standard `CherryPy configuration <http://docs.cherrypy.org/en/latest/config.html>`_ applies,
and the provided `custom tools <#tools>`_ are also configurable.
All request and response bodies are `application/json values <http://tools.ietf.org/html/rfc4627.html#section-2.1>`_.

WebSearcher exposes resources for an IndexSearcher.
In addition to search requests, it provides access to term and document information in the index.

 * :meth:`/ <WebSearcher.index>`
 * :meth:`/search <WebSearcher.search>`
 * :meth:`/docs <WebSearcher.docs>`
 * :meth:`/terms <WebSearcher.terms>`
 * :meth:`/update <WebSearcher.update>`
 * :meth:`/queries <WebSearcher.queries>`

WebIndexer extends WebSearcher, exposing additional resources and methods for an Indexer.
Single documents may be added, deleted, or replaced by a unique indexed field.
Multiples documents may also be added or deleted by query at once.
By default changes are not visible until the update resource is called to commit a new index version.
If a near real-time Indexer is used, then changes are instantly searchable.
In such cases a commit still hasn't occurred, and the index based :meth:`last-modified header <validate>` shouldn't be used for caching.

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

import argparse
import collections
import contextlib
import heapq
import http
import itertools
import os
import re
import time
import warnings
import lucene
import cherrypy
import clients
from requests.compat import json
from lupyne import engine

cherrypy.tools.params._priority = 15  # fix for json_out compatibility


def HTTPError(exception, status=http.client.BAD_REQUEST):
    return cherrypy.HTTPError.handle(exception, int(status))


@cherrypy.tools.register('before_request_body')
def json_in(process_body=None, **kwargs):
    """Handle request bodies in json format.

    :param content_type: request media type
    :param process_body: optional function to process body into request.params
    """
    request = cherrypy.serving.request

    def processor(entity):
        cherrypy.lib.jsontools.json_processor(entity)
        if process_body is not None:
            with HTTPError(TypeError):
                request.params.update(process_body(request.json))
    cherrypy.lib.jsontools.json_in(force='content-type' in request.headers, processor=processor, **kwargs)


@cherrypy.tools.register('before_handler')
def json_out(content_type='application/json', **kwargs):
    """Handle responses in json format.

    :param content_type: response content-type header
    """
    def handler(*args, **kwargs):
        body = cherrypy.request._json_inner_handler(*args, **kwargs)
        return json.dumps(body).encode('utf8') if cherrypy.response.headers['content-type'] == content_type else body
    cherrypy.lib.jsontools.json_out(content_type, handler=handler, **kwargs)


@cherrypy.tools.register('on_start_resource')
def allow(methods=None, paths=(), **kwargs):
    """Only allow specified methods."""
    handler = cherrypy.request.handler
    if paths and hasattr(handler, 'args'):
        with HTTPError(IndexError, http.client.NOT_FOUND):
            methods = paths[len(handler.args)]
    cherrypy.lib.cptools.allow(methods, **kwargs)


@cherrypy.tools.register('before_finalize')
def timer():
    """Return response time in headers."""
    response = cherrypy.serving.response
    response.headers['x-response-time'] = time.time() - response.time


@cherrypy.tools.register('on_start_resource')
def validate(etag=True, last_modified=False, max_age=None, expires=None):
    """Return and validate caching headers.

    :param etag: return weak entity tag header based on index version and validate if-match headers
    :param last_modified: return last-modified header based on index timestamp and validate if-modified headers
    :param max_age: return cache-control max-age and age headers based on last update timestamp
    :param expires: return expires header offset from last update timestamp
    """
    root = cherrypy.request.app.root
    headers = cherrypy.response.headers
    if etag:
        headers['etag'] = root.etag
        cherrypy.lib.cptools.validate_etags()
    if last_modified:
        headers['last-modified'] = cherrypy.lib.httputil.HTTPDate(root.searcher.timestamp)
        cherrypy.lib.cptools.validate_since()
    if max_age is not None:
        headers['age'] = int(time.time() - root.updated)
        headers['cache-control'] = 'max-age={}'.format(max_age)
    if expires is not None:
        headers['expires'] = cherrypy.lib.httputil.HTTPDate(expires + root.updated)


def multi(value):
    return value and value.split(',')


class parse:
    """Parameter parsing."""
    @staticmethod
    def q(searcher, q, **options):
        options = {key.partition('.')[-1]: options[key] for key in options if key.startswith('q.')}
        field = options.pop('field', [])
        fields = [field] if isinstance(field, str) else field
        fields = [name.partition('^')[::2] for name in fields]
        if any(boost for name, boost in fields):
            field = {name: float(boost or 1.0) for name, boost in fields}
        elif isinstance(field, str):
            (field, boost), = fields
        else:
            field = [name for name, boost in fields] or ''
        if 'type' in options:
            with HTTPError(AttributeError):
                return getattr(engine.Query, options['type'])(field, q)
        for key in set(options) - {'op', 'version'}:
            with HTTPError(ValueError):
                options[key] = json.loads(options[key])
        if q is not None:
            with HTTPError(lucene.JavaError):
                return searcher.parse(q, field=field, **options)

    @staticmethod
    def fields(searcher, fields=None, **options):
        if fields is not None:
            fields = dict.fromkeys(fields)
        multi = set(options.get('fields.multi', ()))
        docvalues = dict(parse.docvalues(searcher, field) for field in options.get('fields.docvalues', ()))
        return fields, multi, docvalues

    @staticmethod
    def docvalues(searcher, field):
        name, type = field.split(':') if ':' in field else (field, '')
        with HTTPError(AttributeError):
            return name, searcher.docvalues(name, getattr(__builtins__, type, None))


def json_error(version, **body):
    """Transform errors into json format."""
    tool = cherrypy.request.toolmaps['tools'].get('json_out', {})
    cherrypy.response.headers['content-type'] = tool.get('content_type', 'application/json')
    return json.dumps(body).encode('utf8')


def attach_thread(id=None):
    """Attach current cherrypy worker thread to lucene VM."""
    lucene.getVMEnv().attachCurrentThread()


class Autoreloader(cherrypy.process.plugins.Autoreloader):
    """Autoreload monitor compatible with lucene VM."""
    def run(self):
        attach_thread()
        cherrypy.process.plugins.Autoreloader.run(self)


class AttachedMonitor(cherrypy.process.plugins.Monitor):
    """Periodically run a callback function in an attached thread."""
    def __init__(self, bus, callback, frequency=cherrypy.process.plugins.Monitor.frequency):
        def run():
            attach_thread()
            callback()
        cherrypy.process.plugins.Monitor.__init__(self, bus, run, frequency)

    def subscribe(self):
        cherrypy.process.plugins.Monitor.subscribe(self)
        if cherrypy.engine.state == cherrypy.engine.states.STARTED:
            self.start()

    def unsubscribe(self):
        cherrypy.process.plugins.Monitor.unsubscribe(self)
        self.thread.cancel()


class WebSearcher(object):
    """Dispatch root with a delegated Searcher.

    :param urls: ordered hosts to synchronize with
    """
    _cp_config = {
        'tools.gzip.on': True, 'tools.gzip.mime_types': ['text/html', 'text/plain', 'application/json'],
        'tools.accept.on': True, 'tools.accept.media': 'application/json',
        'tools.json_in.on': True, 'tools.json_out.on': True,
        'tools.allow.on': True, 'tools.timer.on': True,
        'tools.validate.on': True, 'error_page.default': json_error,
    }

    def __init__(self, *directories, **kwargs):
        self.urls = collections.deque(kwargs.pop('urls', ()))
        if self.urls:
            engine.IndexWriter(*directories).close()
        self.searcher = engine.MultiSearcher(directories, **kwargs) if len(directories) > 1 else engine.IndexSearcher(*directories, **kwargs)
        self.updated = time.time()
        self.query_map = {}

    @classmethod
    def new(cls, *args, **kwargs):
        """Return new uninitialized root which can be mounted on dispatch tree before VM initialization."""
        self = object.__new__(cls)
        self.args, self.kwargs = args, kwargs
        return self

    def close(self):
        self.searcher.close()

    @property
    def etag(self):
        return 'W/"{}"'.format(self.searcher.version)

    def sync(self, url):
        """Sync with remote index."""
        directory = self.searcher.path
        resource = clients.Resource(url, headers={'if-none-match': self.etag})
        response = resource.client.put('update/snapshot')
        if response.status_code in (http.client.PRECONDITION_FAILED, http.client.METHOD_NOT_ALLOWED):
            return []
        response.raise_for_status()
        names = sorted(set(response.json()).difference(os.listdir(directory)))
        resource /= response.headers['location']
        try:
            for name in names:
                with open(os.path.join(directory, name), 'wb') as file:
                    resource.download(file, name)
        finally:
            resource.delete()
        return names

    @cherrypy.expose
    @cherrypy.tools.json_in(process_body=dict)
    @cherrypy.tools.allow(methods=['GET', 'POST'])
    def index(self, url=''):
        """Return index information and synchronize with remote index.

        **GET, POST** /[index]
            Return a mapping of the directory to the document count.
            Add new segments from remote host.

            {"url": *string*}

            :return: {*string*: *int*,... }
        """
        if cherrypy.request.method == 'POST':
            self.sync(url)
            cherrypy.response.status = int(http.client.ACCEPTED)
        if isinstance(self.searcher, engine.MultiSearcher):
            return {reader.directory().toString(): reader.numDocs() for reader in self.searcher.indexReaders}
        return {self.searcher.directory.toString(): len(self.searcher)}

    @cherrypy.expose
    @cherrypy.tools.json_in(process_body=dict)
    @cherrypy.tools.allow(methods=['POST'])
    def update(self, **caches):
        """Refresh index version.

        **POST** /update
            Reopen searcher, optionally reloading caches, and return document count.

            {"spellcheckers": true,... }

            .. versionchanged:: 1.2 request body is an object instead of an array

            :return: *int*
        """
        names = ()
        while self.urls:
            url = self.urls[0]
            try:
                names = self.sync(url)
                break
            except IOError:
                with contextlib.suppress(ValueError):
                    self.urls.remove(url)
        self.searcher = self.searcher.reopen(**caches)
        self.updated = time.time()
        if names:
            engine.IndexWriter(self.searcher.directory).close()
        if not self.urls and hasattr(self, 'fields'):
            other = WebIndexer(self.searcher.directory, analyzer=self.searcher.analyzer)
            other.indexer.shared, other.indexer.fields = self.searcher.shared, self.fields
            app, = (app for app in cherrypy.tree.apps.values() if app.root is self)
            mount(other, app=app, autoupdate=getattr(self, 'autoupdate', 0))
        return len(self.searcher)

    @cherrypy.expose
    @cherrypy.tools.params()
    def docs(self, name=None, value='', **options):
        """Return ids or documents.

        **GET** /docs
            Return array of doc ids.

            :return: [*int*,... ]

        **GET** /docs/[*int*\|\ *chars*/*chars*]?
            Return document mapping from id or unique name and value.

            &fields=\ *chars*,... &fields.multi=\ *chars*,... &fields.docvalues=\ *chars*\ [:*chars*],...
                optionally select stored, multi-valued, and docvalues

            &fields.vector=\ *chars*,... &fields.vector.counts=\ *chars*,...
                optionally select term vectors with term counts

            :return: {*string*: null|\ *string*\|\ *number*\|\ *array*\|\ *object*,... }
        """
        searcher = self.searcher
        if not name:
            return list(searcher)
        with HTTPError(ValueError, http.client.NOT_FOUND):
            id, = searcher.docs(name, value) if value else [int(name)]
        fields, multi, docvalues = parse.fields(searcher, **options)
        with HTTPError(lucene.JavaError, http.client.NOT_FOUND):
            doc = searcher[id] if fields is None else searcher.get(id, *itertools.chain(fields, multi))
        result = doc.dict(*multi, **(fields or {}))
        with HTTPError(TypeError):
            result.update((name, docvalues[name][id]) for name in docvalues)
        result.update((field, list(searcher.termvector(id, field))) for field in options.get('fields.vector', ()))
        result.update((field, dict(searcher.termvector(id, field, counts=True))) for field in options.get('fields.vector.counts', ()))
        return result
    docs.__annotations__.update(dict.fromkeys(['fields', 'fields.multi', 'fields.docvalues', 'fields.vector', 'fields.vector.counts'], multi))

    @cherrypy.expose
    @cherrypy.tools.params()
    def search(self, q=None, count: int = None, start: int = 0, fields: multi = None, sort: multi = None,
               facets: multi = '', group='', hl: multi = '', mlt: int = None, timeout: float = None, **options):
        """Run query and return documents.

        **GET** /search?
            Return array of document objects and total doc count.

            &q=\ *chars*\ &q.type=[term|prefix|wildcard]&q.spellcheck=true&q.\ *chars*\ =...,
                query, optional type to skip parsing, spellcheck, and parser settings: q.field, q.op,...

            &count=\ *int*\ &start=0
                maximum number of docs to return and offset to start at

            &fields=\ *chars*,... &fields.multi=\ *chars*,... &fields.docvalues=\ *chars*\ [:*chars*],...
                only include selected stored fields; multi-valued fields returned in an array; docvalues fields

            &sort=\ [-]\ *chars*\ [:*chars*],... &sort.scores[=max]
                | field name, optional type, minus sign indicates descending
                | optionally score docs, additionally compute maximum score

            &facets=\ *chars*,... &facets.count=\ *int*\&facets.min=0
                | include facet counts for given field names
                | optional maximum number of most populated facet values per field, and minimum count to return

            &group=\ *chars*\ [:*chars*]&group.count=1
                | group documents by field value with optional type, up to given maximum count

            .. versionchanged:: 1.6 grouping searches use count and start options

            &hl=\ *chars*,... &hl.count=1
                | stored fields to return highlighted
                | optional maximum fragment count

            &mlt=\ *int*\ &mlt.fields=\ *chars*,... &mlt.\ *chars*\ =...,
                | doc index (or id without a query) to find MoreLikeThis
                | optional document fields to match
                | optional MoreLikeThis settings: mlt.minTermFreq, mlt.minDocFreq,...

            &timeout=\ *number*
                timeout search after elapsed number of seconds

            :return:
                | {
                | "query": *string*\|null,
                | "count": *int*\|null,
                | "maxscore": *number*\|null,
                | "docs": [{"__id__": *int*, "__score__": *number*, "__keys__": *array*,
                    "__highlights__": {*string*: *array*,... }, *string*: *value*,... },... ],
                | "facets": {*string*: {*string*: *int*,... },... },
                | "groups": [{"count": *int*, "value": *value*, "docs": [*object*,... ]},... ]
                | }
        """
        searcher = self.searcher
        if sort is not None:
            sort = (re.match('(-?)(\w+):?(\w*)', field).groups() for field in sort)
            with HTTPError(AttributeError):
                sort = [searcher.sortfield(name, getattr(__builtins__, type, None), (reverse == '-')) for reverse, name, type in sort]
        q = parse.q(searcher, q, **options)
        if mlt is not None:
            if q is not None:
                mlt, = searcher.search(q, count=mlt + 1, sort=sort)[mlt:].ids
            mltfields = options.pop('mlt.fields', ())
            with HTTPError(ValueError):
                attrs = {key.partition('.')[-1]: json.loads(options[key]) for key in options if key.startswith('mlt.')}
            q = searcher.morelikethis(mlt, *mltfields, analyzer=searcher.analyzer, **attrs)
        if count is not None:
            count += start
        if count == 0:
            start = count = 1
        scores = options.get('sort.scores')
        gcount = options.get('group.count', 1)
        scores = {'scores': scores is not None, 'maxscore': scores == 'max'}
        if ':' in group:
            hits = searcher.search(q, sort=sort, timeout=timeout, **scores)
            name, docvalues = parse.docvalues(searcher, group)
            with HTTPError(TypeError):
                groups = hits.groupby(docvalues.select(hits.ids).__getitem__, count=count, docs=gcount)
            groups.groupdocs = groups.groupdocs[start:]
        elif group:
            scores = {'includeScores': scores['scores'], 'includeMaxScore': scores['maxscore']}
            groups = searcher.groupby(group, q, count, start=start, sort=sort, groupDocsLimit=gcount, **scores)
        else:
            hits = searcher.search(q, sort=sort, count=count, timeout=timeout, **scores)
            groups = engine.documents.Groups(searcher, [hits[start:]], hits.count, hits.maxscore)
        result = {'query': q and str(q), 'count': groups.count, 'maxscore': groups.maxscore}
        fields, multi, docvalues = parse.fields(searcher, fields, **options)
        if fields is None:
            fields = {}
        else:
            groups.select(*itertools.chain(fields, multi))
        hl = dict.fromkeys(hl, options.get('hl.count', 1))
        result['groups'] = []
        for hits in groups:
            docs = []
            highlights = hits.highlights(q, **hl) if hl else ([{}] * len(hits))
            for hit, highlight in zip(hits, highlights):
                doc = hit.dict(*multi, **fields)
                with HTTPError(TypeError):
                    doc.update((name, docvalues[name][hit.id]) for name in docvalues)
                if highlight:
                    doc['__highlights__'] = highlight
                docs.append(doc)
            result['groups'].append({'docs': docs, 'count': hits.count, 'value': getattr(hits, 'value', None)})
        if not group:
            result['docs'] = result.pop('groups')[0]['docs']
        q = q or engine.Query.alldocs()
        if facets:
            query_map = {facet: self.query_map[facet] for facet in set(facets).intersection(self.query_map)}
            facets = result['facets'] = searcher.facets(q, *set(facets).difference(query_map), **query_map)
            for counts in facets.values():
                counts.pop(None, None)
            if 'facets.min' in options:
                for name, counts in facets.items():
                    facets[name] = {term: count for term, count in counts.items() if count >= options['facets.min']}
            if 'facets.count' in options:
                for name, counts in facets.items():
                    facets[name] = {term: counts[term] for term in heapq.nlargest(options['facets.count'], counts, key=counts.__getitem__)}
        return result
    search.__annotations__.update({'fields.multi': multi, 'fields.docvalues': multi, 'facets.count': int, 'facets.min': int,
                                   'group.count': int, 'hl.count': int, 'mlt.fields': multi})

    @cherrypy.expose
    @cherrypy.tools.params()
    def terms(self, name='', value='*', *path, count: int = 0):
        """Return data about indexed terms.

        **GET** /terms?
            Return field names.

            :return: [*string*,... ]

        **GET** /terms/*chars*
            Return term values for given field name.

            :return: [*string*,... ]

        **GET** /terms/*chars*/*chars*\[\*\|:*chars*\|~[\ *int*\]]
            Return term values (prefix, slices, or fuzzy terms) for given field name.

            :return: [*string*,... ]

        **GET** /terms/*chars*/*chars*\[\*\|~[\ *int*\]\]?count=\ *int*
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
            return sorted(fieldinfo.name for fieldinfo in searcher.fieldinfos.values())
        if ':' in value:
            return list(searcher.terms(name, *value.split(':')))
        if value.endswith('*'):
            value = value.rstrip('*')
            if count:
                return searcher.complete(name, value, count)
            return list(searcher.terms(name, value))
        if '~' in value:
            with HTTPError(ValueError):
                value, distance = value.split('~')
                distance = int(distance or 2)
            if count:
                return searcher.suggest(name, value, count, maxEdits=distance)
            return list(searcher.terms(name, value, distance=distance))
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

    @cherrypy.expose
    @cherrypy.tools.allow(paths=[('GET',), ('GET', 'POST'), ('GET', 'PUT', 'DELETE')])
    def queries(self, name='', value=''):
        """Match a document against registered queries.

        Queries are cached by a unique name and value, suitable for document indexing.

        .. versionadded:: 1.4

        **GET** /queries
            Return query set names.

            :return: [*string*,... ]

        **GET, POST** /queries/*chars*
            Return query values and scores which match given document.

            {*string*: *string*,... }

            :return: {*string*: *number*,... }

        **GET, PUT, DELETE** /queries/*chars*/*chars*
            Return, create, or delete a registered query.

            *string*

            :return: *string*
        """
        request = cherrypy.serving.request
        if not name:
            return sorted(self.query_map)
        if not value:
            if request.method == 'GET':
                request.body.process()
            with HTTPError(KeyError, http.client.NOT_FOUND):
                queries = self.query_map[name]
            scores = self.searcher.match(getattr(request, 'json', {}), *queries.values())
            return dict(zip(queries, scores))
        if request.method == 'DELETE':
            return str(self.query_map.get(name, {}).pop(value, '')) or None
        if request.method == 'PUT':
            queries = self.query_map.setdefault(name, {})
            if value not in queries:
                cherrypy.response.status = int(http.client.CREATED)
            queries[value] = self.searcher.parse(request.json)
        with HTTPError(KeyError, http.client.NOT_FOUND):
            return str(self.query_map[name][value])


class WebIndexer(WebSearcher):
    """Dispatch root with a delegated Indexer, exposing write methods."""
    def __init__(self, *args, **kwargs):
        self.indexer = engine.Indexer(*args, **kwargs)
        self.updated = time.time()
        self.query_map = {}

    @property
    def searcher(self):
        return self.indexer.indexSearcher

    def close(self):
        self.indexer.close()
        super().close()

    def refresh(self):
        if self.indexer.nrt:
            self.indexer.refresh()
            self.updated = time.time()
        else:
            cherrypy.response.status = int(http.client.ACCEPTED)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'POST'])
    def index(self):
        """Add indexes.  See :meth:`WebSearcher.index` for GET method.

        **POST** /[index]
            Add indexes without optimization.

            [*string*,... ]
        """
        request = cherrypy.serving.request
        if request.method == 'POST':
            for directory in getattr(request, 'json', ()):
                self.indexer += directory
            self.refresh()
        return {str(self.indexer.directory): len(self.indexer)}

    @cherrypy.expose
    @cherrypy.tools.json_in(process_body=dict)
    @cherrypy.tools.allow(paths=[('POST',), ('GET', 'PUT', 'DELETE'), ('GET',)])
    def update(self, id='', name='', **options):
        """Commit index changes and refresh index version.

        **POST** /update
            Commit write operations and return document count.  See :meth:`WebSearcher.update` for caching options.

            {"merge": true|\ *int*,... }

            .. versionchanged:: 1.2 request body is an object instead of an array

            :return: *int*

        **GET, PUT, DELETE** /update/[snapshot|\ *int*]
            Verify, create, or release unique snapshot of current index commit and return array of referenced filenames.

            .. versionchanged:: 1.4 lucene identifies snapshots by commit generation;  use location header

            :return: [*string*,... ]

        **GET** /update/*int*/*chars*
            Download index file corresponding to snapshot id and filename.
        """
        if not id:
            self.indexer.commit(**options)
            self.updated = time.time()
            return len(self.indexer)
        method = cherrypy.request.method
        response = cherrypy.serving.response
        if method == 'PUT':
            if id != 'snapshot':
                raise cherrypy.NotFound()
            commit = self.indexer.policy.snapshot()
            response.status = int(http.client.CREATED)
            response.headers['location'] = cherrypy.url('/update/{0:d}'.format(commit.generation), relative='server')
        else:
            with HTTPError((ValueError, AssertionError), http.client.NOT_FOUND):
                commit = self.indexer.policy.getIndexCommit(int(id))
                assert commit is not None, 'commit not snapshotted'
            if method == 'DELETE':
                self.indexer.policy.release(commit)
        if not name:
            return list(commit.fileNames)
        with HTTPError((TypeError, AssertionError), http.client.NOT_FOUND):
            directory = self.searcher.path
            assert name in commit.fileNames, 'file not referenced in commit'
        return cherrypy.lib.static.serve_download(os.path.join(directory, name))

    @cherrypy.expose
    @cherrypy.tools.allow(paths=[('GET', 'POST'), ('GET',), ('GET', 'PUT', 'DELETE', 'PATCH')])
    def docs(self, name=None, value='', **options):
        """Add or return documents.  See :meth:`WebSearcher.docs` for GET method.

        **POST** /docs
            Add documents to index.

            [{*string*: *string*\|\ *number*\|\ *array*,... },... ]

        **PUT, DELETE** /docs/*chars*/*chars*
            Set or delete document.  Unique term should be indexed and is added to the new document.

            {*string*: *string*\|\ *number*\|\ *array*,... }
        """
        request = cherrypy.serving.request
        if request.method in ('GET', 'HEAD'):
            return super().docs(name, value, **options)
        if request.method == 'DELETE':
            self.indexer.delete(name, value)
        elif request.method == 'POST':
            for doc in getattr(request, 'json', ()):
                self.indexer.add(doc)
        else:
            doc = getattr(request, 'json', {})
            with HTTPError((KeyError, AssertionError), http.client.CONFLICT):
                assert self.indexer.fields[name].indexed, 'unique field must be indexed'
            if request.method == 'PUT':
                with HTTPError(AssertionError):
                    assert doc.setdefault(name, value) == value, 'multiple values for unique field'
            else:
                with HTTPError((KeyError, AssertionError), http.client.CONFLICT):
                    assert all(self.indexer.fields[name].docvalues for name in doc)
            self.indexer.update(name, value, doc)
        self.refresh()
    docs._cp_config.update(WebSearcher.docs._cp_config)
    docs._cp_config['request.methods_with_bodies'] = ('POST', 'PUT', 'PATCH')
    docs.__annotations__.update(WebSearcher.docs.__annotations__)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'DELETE'])
    def search(self, q=None, **options):
        """Run or delete a query.  See :meth:`WebSearcher.search` for GET method.

        **DELETE** /search?q=\ *chars*
            Delete documents which match query.
        """
        if cherrypy.request.method != 'DELETE':
            return super().search(q, **options)
        if q is None:
            self.indexer.deleteAll()
        else:
            self.indexer.delete(parse.q(self.searcher, q, **options))
        self.refresh()
    search._cp_config.update(WebSearcher.search._cp_config)
    search.__annotations__.update(WebSearcher.search.__annotations__)

    @cherrypy.expose
    @cherrypy.tools.json_in(process_body=dict)
    @cherrypy.tools.allow(paths=[('GET',), ('GET', 'PUT')])
    @cherrypy.tools.validate(on=False)
    def fields(self, name='', **settings):
        """Return or store a field's settings.

        **GET** /fields
            Return known field names.

            :return: [*string*,... ]

        **GET, PUT** /fields/*chars*
            Set and return settings for given field name.

            {"stored"|"indexOptions"\|...: *string*\|true|false,... }

            .. versionchanged:: 1.6 lucene FieldType attributes used as settings

            :return: {"stored"|"indexOptions"\|...: *string*\|true|false,... }
        """
        if not name:
            return sorted(self.indexer.fields)
        if cherrypy.request.method == 'PUT':
            if name not in self.indexer.fields:
                cherrypy.response.status = int(http.client.CREATED)
            with HTTPError(AttributeError):
                self.indexer.set(name, **settings)
        with HTTPError(KeyError, http.client.NOT_FOUND):
            return self.indexer.fields[name].settings


def init(vmargs='-Xrs,-Djava.awt.headless=true', **kwargs):
    """Callback to initialize VM and app roots after daemonizing."""
    assert lucene.getVMEnv() or lucene.initVM(vmargs=vmargs, **kwargs)
    for app in cherrypy.tree.apps.values():
        if isinstance(app.root, WebSearcher):
            app.root.__init__(*app.root.__dict__.pop('args'), **app.root.__dict__.pop('kwargs'))


def mount(root, path='', config=None, autoupdate=0, app=None):
    """Attach root and subscribe to plugins.

    :param root,path,config: see cherrypy.tree.mount
    :param autoupdate: see command-line options
    :param app: optionally replace root on existing app
    """
    if app is None:
        app = cherrypy.tree.mount(root, path, config)
    else:
        cherrypy.engine.unsubscribe('stop', app.root.close)
        if hasattr(app.root, 'monitor'):
            app.root.monitor.unsubscribe()
        app.root = root
    cherrypy.engine.subscribe('stop', root.close)
    if autoupdate:
        root.monitor = AttachedMonitor(cherrypy.engine, root.update, autoupdate)
        root.monitor.subscribe()
    return app


def start(root=None, path='', config=None, pidfile='', daemonize=False, autoreload=0, autoupdate=0, callback=None):
    """Attach root, subscribe to plugins, and start server.

    :param root,path,config: see cherrypy.quickstart
    :param pidfile,daemonize,autoreload,autoupdate: see command-line options
    :param callback: optional callback function scheduled after daemonizing
    """
    cherrypy.engine.subscribe('start_thread', attach_thread)
    cherrypy.config['engine.autoreload.on'] = False
    if pidfile:
        cherrypy.process.plugins.PIDFile(cherrypy.engine, os.path.abspath(pidfile)).subscribe()
    if daemonize:
        cherrypy.config['log.screen'] = False
        cherrypy.process.plugins.Daemonizer(cherrypy.engine).subscribe()
    if autoreload:
        reloader = Autoreloader(cherrypy.engine, autoreload, match='lupyne.*')
        reloader.files.add(__file__)
        reloader.subscribe()
    if callback:
        priority = (cherrypy.process.plugins.Daemonizer.start.priority + cherrypy.process.plugins.Monitor.start.priority) // 2
        cherrypy.engine.subscribe('start', callback, priority)
    if root is not None:
        mount(root, path, config, autoupdate)
    cherrypy.quickstart(cherrypy.tree.apps.get(path), path, config)


parser = argparse.ArgumentParser(description='Restful json cherrypy server.', prog='lupyne.server')
parser.add_argument('directories', nargs='*', metavar='directory', help='index directories')
parser.add_argument('-r', '--read-only', action='store_true', help='expose only read methods; no write lock')
parser.add_argument('-c', '--config', help='optional configuration file or json object of global params')
parser.add_argument('-p', '--pidfile', metavar='FILE', help='store the process id in the given file')
parser.add_argument('-d', '--daemonize', action='store_true', help='run the server as a daemon')
parser.add_argument('--autoreload', type=float, metavar='SECONDS', help='automatically reload modules; replacement for engine.autoreload')
parser.add_argument('--autoupdate', type=float, metavar='SECONDS', help='automatically update index version and commit any changes')
parser.add_argument('--autosync', metavar='URL,...', help='automatically synchronize searcher with remote hosts and update')
parser.add_argument('--real-time', action='store_true', help='search in real-time without committing')

if __name__ == '__main__':
    args = parser.parse_args()
    read_only = args.read_only or args.autosync or len(args.directories) > 1
    kwargs = {'nrt': True} if args.real_time else {}
    if read_only and (args.real_time or not args.directories):
        parser.error('incompatible read/write options')
    if args.autosync:
        kwargs['urls'] = args.autosync.split(',')
        if not (args.autoupdate and len(args.directories) == 1):
            parser.error('autosync requires autoupdate and a single directory')
        warnings.warn('autosync is not recommended for production usage')
    if args.config and not os.path.exists(args.config):
        args.config = {'global': json.loads(args.config)}
    cls = WebSearcher if read_only else WebIndexer
    root = cls.new(*map(os.path.abspath, args.directories), **kwargs)
    del args.directories, args.read_only, args.autosync, args.real_time
    start(root, callback=init, **args.__dict__)
