"""
Restful json server.

Autoreload is not compatible with the VM initialization that lucene requires.
Also recommended that lucene VM ignores keyboard interrupts.
"""

import itertools, operator
import json
import thread
from functools import partial
import lucene
import cherrypy
from cherrypy.wsgiserver import WorkerThread
from engine import Indexer, Hit

def json_tool():
    if cherrypy.response.status is None:
        cherrypy.response.headers['Content-Type'] = 'text/json'
        cherrypy.response.body = json.dumps(cherrypy.response.body)
cherrypy.tools.json = cherrypy.Tool('before_finalize', json_tool)

class AttachedThread(WorkerThread):
    "Attach cherrypy threads to lucene VM."
    def run(self):
        lucene.getVMEnv().attachCurrentThread()
        WorkerThread.run(self)

def getitem(obj, key):
    "Get item or raise NotFound."
    try:
        return obj[key]
    except KeyError:
        raise cherrypy.NotFound(key)

def json_doc(doc, fields=None, multifields=()):
    "Transform document into json compatible dict."
    if fields is None:
        result = dict(doc.items())
    else:
        result = dict(zip(fields, map(doc.__getitem__, fields)))
    if isinstance(doc, Hit):
        result.update(itertools.islice(doc.items(), 2))  # id & score
    result.update(zip(multifields, map(doc.getlist, multifields)))
    return result

class Root(object):
    def __init__(self, *args, **kwargs):
        self.indexer = Indexer(*args, **kwargs)
        self.lock = thread.allocate_lock()
    @cherrypy.expose
    def index(self):
        """Return index information.
        
        GET /   {directory<string>, count<int>}"""
        return {str(self.indexer.directory): len(self.indexer)}
    @cherrypy.expose
    def commit(self):
        """Commit write operations.
        
        POST /commit"""
        with self.lock:
            self.indexer.commit()
    @cherrypy.expose
    def docs(self, id=None, docs='[]', fields=None, multifields=''):
        """Return and index documents.
        
        GET /docs   [id<int>,... ]
        GET /docs/<int>?
            &fields=<chars>,...
            &multifields=<chars>,...
        {<string>: <string>|<array>,... }
        POST /docs  docs=[{<string>: <string>|<array>,... },... ]"""
        if fields is not None:
            fields = fields.split(',')
        multifields = filter(None, multifields.split(','))
        if cherrypy.request.method == 'GET':
            if id is None:
                return list(self.indexer)
            return json_doc(getitem(self.indexer, int(id)), fields, multifields)
        for doc in json.loads(docs):
            self.indexer.add(doc)
    @cherrypy.expose
    def search(self, q, count=None, fields=None, multifields='', sort=None, reverse='false'):
        """Run or delete a query.
        
        DELETE /search?q=<chars>
        GET /search?q=<chars>
            &count=<int>,
            &fields=<chars>,...
            &multifields=<chars>,...
            &sort=<chars>,...
            &reverse=true|false,
        {"count": <int>, "docs": [{"__id__": <int>, "__score__": <int>, <string>: <string>|<array>,... },... ]}"""
        if cherrypy.request.method == 'DELETE':
            return self.indexer.delete(q)
        if count is not None:
            count = int(count)
        if fields is not None:
            fields = fields.split(',')
        multifields = filter(None, multifields.split(','))
        if sort is not None and ',' in sort:
            sort = fields.split(',')
        reverse = json.loads(reverse)
        hits = self.indexer.search(q, count=count, sort=sort)
        docs = [json_doc(hit, fields, multifields) for hit in hits]
        return {'count': hits.count, 'docs': docs}
    @cherrypy.expose
    def fields(self, name='', **settings):
        """Return data about fields and assign their settings.
        
        GET /fields                     [name<string>,... ]
        GET/POST /fields/name<chars>    {store=<string>, index=<string>, termvector=<string>}"""
        if not name:
            return sorted(self.indexer.settings)
        if cherrypy.request.method in ('PUT', 'POST'):
            self.indexer.set(name, **settings)
        return dict(zip(['store', 'index', 'termvector'], map(str, getitem(self.indexer.settings, name))))
    @cherrypy.expose
    def terms(self, name='', value=':', *args, **options):
        """Return data about indexed terms.
        
        GET /terms?option=<chars>                               [name<string>,... ]
        GET /terms/name<chars>/[start<chars>:stop<chars>]       [value<string>,... ]
        GET /terms/name<chars>/value<chars>                     count<int>
        GET /terms/name<chars>/value<chars>/docs                [id<int>,... ]
        GET /terms/name<chars>/value<chars>/docs/counts         [[id<int>, count<int>],... ]
        GET /terms/name<chars>/value<chars>/docs/positions      [[id<int>, [position<int>,... ]],... ]"""
        if not name:
            return sorted(self.indexer.fields(**options))
        if ':' in value:
            start, stop = value.split(':')
            terms = self.indexer.terms(name, start)
            if stop:
                terms = itertools.takewhile(partial(operator.gt, stop), terms)
            return list(terms)
        docs, stats = (args + ('', ''))[:2]
        if not docs:
            return self.indexer.count(name, value)
        if docs == 'docs':
            if stats == 'positions':
                return list(self.indexer.positions(name, value))
            if stats in ('', 'counts'):
                return list(self.indexer.docs(name, value, counts=bool(stats)))
        raise cherrypy.NotFound('/'.join(args))

def main(root):
    "Attach root and run server."
    cherrypy.wsgiserver.WorkerThread = AttachedThread
    cherrypy.engine.subscribe('stop', root.indexer.close)
    cherrypy.config['engine.autoreload.on'] = False
    cherrypy.config['tools.json.on'] = True
    cherrypy.config['checker.on'] = False # python2.6 incompatibility
    cherrypy.server.socket_host = '0.0.0.0'
    cherrypy.quickstart(root)

if __name__ == '__main__':
    import sys
    if lucene.getVMEnv() is None:
        lucene.initVM(lucene.CLASSPATH, vmargs='-Xrs')
    main(Root(*sys.argv[1:]))
