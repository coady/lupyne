"""
Pythonic wrapper around lucene search engine.

Provides high-level interfaces to indexes and documents,
abstracting away java lucene primitives: terms, fields, etc.
"""

import itertools, operator
import warnings
import contextlib
import lucene

if lucene.getVMEnv() is None:
    warnings.warn("lucene.initVM(lucene.CLASSPATH,... ) must be called before using lucene.", RuntimeWarning, stacklevel=2)

def iterate(jit, positioned=False):
    """Transform java iterator into python iterator.
    
    @positioned: current iterator position is valid."""
    with contextlib.closing(jit):
        if positioned:
            yield jit
        while jit.next():
            yield jit

class Document(object):
    """Delegated lucene Document.
    
    Supports mapping interface of field names to values, but duplicate field names are allowed."""
    Fields = lucene.Field.Store, lucene.Field.Index, lucene.Field.TermVector
    def __init__(self, doc=None):
        self.doc = lucene.Document() if doc is None else doc
    @classmethod
    def settings(cls, store=False, index='analyzed', termvector=False):
        "Return field parameters from text or boolean descriptors."
        if isinstance(store, bool):
            store = 'yes' if store else 'no'
        if isinstance(index, bool):
            index = 'not_analyzed' if index else 'no'
        if isinstance(termvector, bool):
            termvector = 'yes' if termvector else 'no'
        return [getattr(field, setting.upper()) for field, setting in zip(cls.Fields, [store, index, termvector])]
    def add(self, name, value, **settings):
        "Add field to document with given parameters."
        self.doc.add(lucene.Field(name, value, *self.settings(**settings)))
    def fields(self):
        return itertools.imap(lucene.Field.cast_, self.doc.getFields())
    def __iter__(self):
        for field in self.fields():
            yield field.name()
    def items(self):
        for field in self.fields():
            yield field.name(), field.stringValue()
    def __getitem__(self, name):
        value = self.doc[name]
        if value is None:
            raise KeyError(value)
        return value
    def get(self, name, default=None):
        value = self.doc[name]
        return default if value is None else value
    def __delitem__(self, name):
        self.doc.removeFields(name)
    def getlist(self, name):
        "Return multiple field values."
        return list(self.doc.getValues(name))

class IndexReader(object):
    """Delegated lucene IndexReader.
    
    Supports mapping interface of doc ids to document objects.
    @directory: lucene IndexReader or directory."""
    def __init__(self, directory):
        self.indexReader = directory if isinstance(directory, lucene.IndexReader) else lucene.IndexReader.open(directory)
    def __getattr__(self, name):
        if name == 'indexReader':
            raise AttributeError(name)
        return getattr(self.indexReader, name)
    def __len__(self):
        return self.numDocs()
    def __contains__(self, id):
        return 0 <= id < self.maxDoc() and not self.isDeleted(id)
    def __iter__(self):
        return itertools.ifilterfalse(self.isDeleted, xrange(self.maxDoc()))
    def __getitem__(self, id):
        try:
            return Document(self.document(id))
        except lucene.JavaError:
            raise KeyError(id)
    def __delitem__(self, id):
        self.deleteDocument(id)
    @property
    def directory(self):
        "Return reader's directory."
        return self.indexReader.directory()
    def delete(self, name, value):
        "Delete documents with given term."
        self.deleteDocuments(lucene.Term(name, value))
    def count(self, name, value):
        "Return number of documents with given term."
        return self.docFreq(lucene.Term(name, value))
    def fields(self, option='all'):
        "Return field names, given option description."
        option = getattr(self.FieldOption, option.upper())
        return list(self.getFieldNames(option))
    def terms(self, name, value='', counts=False):
        "Generate term values given starting point, optionally with frequency counts."
        for termenum in iterate(self.indexReader.terms(lucene.Term(name, value)), positioned=True):
            term = termenum.term()
            if not term or term.field() != name:
                break
            text = term.text()
            yield (text, termenum.docFreq()) if counts else text
    def docs(self, name, value, counts=False):
        "Generate doc ids which contain given term, optionally with frequency counts."
        for termdocs in iterate(self.termDocs(lucene.Term(name, value))):
            doc = termdocs.doc()
            yield (doc, termdocs.freq()) if counts else doc
    def positions(self, name, value):
        "Generate doc ids which contain given term, with their positions."
        for termpositions in iterate(self.termPositions(lucene.Term(name, value))):
            positions = [termpositions.nextPosition() for n in xrange(termpositions.freq())]
            yield termpositions.doc(), positions

class Hit(Document):
    "A Document with an id and score, from a search result."
    def __init__(self, doc, id, score):
        Document.__init__(self, doc)
        self.id, self.score = id, score
    def items(self):
        "Include id and score using python naming convention."
        fields = {'__id__': self.id, '__score__': self.score}
        return itertools.chain(fields.items(), Document.items(self))

class Hits(object):
    """Search results: lazily evaluated and memory efficient.
    
    Supports a read-only sequence interface to hit objects.
    @searcher: IndexSearcher which can retrieve documents.
    @ids: ordered doc ids.
    @scores: ordered doc scores.
    @count: total number of hits."""
    def __init__(self, searcher, ids, scores, count=0):
        self.searcher = searcher
        self.ids, self.scores = ids, scores
        self.count = count or len(self)
    def __len__(self):
        return len(self.ids)
    def __getitem__(self, index):
        id, score = self.ids[index], self.scores[index]
        if isinstance(index, slice):
            return type(self)(self.searcher, id, score, self.count)
        return Hit(self.searcher.doc(id), id, score)
    def items(self):
        "Generate zipped ids and scores."
        return itertools.izip(self.ids, self.scores)

class HitCollector(lucene.PythonHitCollector):
    "Collect all ids and scores."
    def __init__(self, searcher):
        lucene.PythonHitCollector.__init__(self, searcher)
        self.collect = {}.__setitem__
    def sorted(self, key=None, reverse=False):
        "Return ordered ids and scores."
        data = self.collect.__self__
        ids = sorted(data)
        if key is None:
            ids.sort(key=data.__getitem__, reverse=True)
        else:
            ids.sort(key=key, reverse=reverse)
        return ids, map(data.__getitem__, ids)

class Filter(lucene.PythonFilter):
    "Filter a set of ids."
    def __init__(self, ids):
        lucene.PythonFilter.__init__(self)
        self.ids = ids
    def bits(self, reader):
        bits = lucene.BitSet(reader.maxDoc())
        for id in self.ids:
            bits.set(id)
        return bits

class IndexSearcher(lucene.IndexSearcher, IndexReader):
    """Inherited lucene IndexSearcher.
    
    Also delegates to IndexReader's interface.
    @directory: directory path or lucene Directory.
    @analyzer: lucene Analyzer class."""
    def __init__(self, directory, analyzer=lucene.StandardAnalyzer):
        lucene.IndexSearcher.__init__(self, directory)
        self.analyzer = analyzer()
    def __del__(self):
        if str(self) != '<null>':
            self.close()
    def parse(self, query, field='', op='or'):
        """Return parsed query.
        
        @field: default query field name.
        @op: default query operator."""
        # parser's aren't thread-safe (nor slow), so create one each time
        parser = lucene.QueryParser(field, self.analyzer)
        parser.defaultOperator = getattr(lucene.QueryParser.Operator, op.upper())
        return parser.parse(query)
    def count(self, *query, **parser):
        "Run number of hits for given query or term."
        if len(query) == 1:
            return self.search(query[0], count=1, **parser).count
        return IndexReader.count(self, *query)
    def search(self, query, filter=None, count=None, sort=None, reverse=False, **parser):
        """Run query and return Hits.
        
        @query: query string or lucene Query.
        @filter: doc ids or lucene Filter.
        @count: maximum number of hits to return.
        @sort: field name, names, or lucene Sort.
        @reverse: reverse flag used with sort.
        @parser: parsing options."""
        if not isinstance(query, lucene.Query):
            query = self.parse(query, **parser)
        if not isinstance(filter, (lucene.Filter, type(None))):
            filter = Filter(filter)
        # use custom HitCollector if all results are necessary, otherwise let lucene's TopDocs handle it
        if count is None:
            collector = HitCollector(self)
            lucene.IndexSearcher.search(self, query, filter, collector)
            return Hits(self, *collector.sorted(key=sort, reverse=reverse))
        if sort is None:
            sort = ()
        else:
            if not isinstance(sort, lucene.Sort):
                sort = lucene.Sort(sort, reverse) if isinstance(sort, basestring) else lucene.Sort(sort)
            sort = sort,
        topdocs = lucene.IndexSearcher.search(self, query, filter, count, *sort)
        ids, scores = (map(operator.attrgetter(name), topdocs.scoreDocs) for name in ['doc', 'score'])
        return Hits(self, ids, scores, topdocs.totalHits)

class IndexWriter(lucene.IndexWriter):
    """Inherited lucene IndexWriter.
    
    Supports setting fields parameters explicitly, so documents can be represented as dictionaries.
    @directory: directory path or lucene Directory.
    @mode: file mode (updating is implied) - 'w' truncates, 'r' doesn't create.
    @analyzer: lucene Analyzer class."""
    __len__ = lucene.IndexWriter.numDocs
    __del__ = IndexSearcher.__del__.im_func
    parse = IndexSearcher.parse.im_func
    def __init__(self, directory=None, mode='a', analyzer=lucene.StandardAnalyzer):
        create = [mode == 'w'] * (mode != 'a')
        lucene.IndexWriter.__init__(self, directory or lucene.RAMDirectory(), analyzer(), *create)
        self.settings = {}
    @property
    def segments(self):
        "Return segment filenames with document counts."
        items = (seg.split(':c') for seg in self.segString().split())
        return dict((name, int(value)) for name, value in items)
    def set(self, name, **settings):
        "Assign settings to field name."
        self.settings[name] = Document.settings(**settings)
    def add(self, document=(), **fields):
        """Add document to index.
        
        @document: optional document dict or items.
        @fields: additional fields to document."""
        fields.update(document)
        doc = lucene.Document()
        for name, values in fields.items():
            settings = self.settings[name]
            for value in ([values] if isinstance(values, basestring) else values):
                doc.add(lucene.Field(name, value, *settings))
        self.addDocument(doc)
    def delete(self, *query, **parser):
        "Remove documents which match given query or term."
        if len(query) == 1:
            query = query[0]
            self.deleteDocuments(query if isinstance(query, lucene.Query) else self.parse(query, **parser))
        else:
            self.deleteDocuments(lucene.Term(*query))
    def __iadd__(self, directory):
        "Add directory (or reader, searcher, writer) to index."
        if isinstance(directory, basestring):
            directory = lucene.FSDirectory(directory)
        elif not isinstance(directory, lucene.Directory):
            directory = directory.directory
        self.addIndexesNoOptimize([directory])
        return self

class Indexer(IndexWriter):
    """A all purpose interface to an index.
    
    Opening in read mode yields in only an IndexSearcher.
    Opening in write mode yields an IndexWriter with a delegated IndexSearcher."""
    def __new__(cls, directory=None, mode='a', analyzer=lucene.StandardAnalyzer):
        if mode == 'r':
            return IndexSearcher(directory, analyzer)
        return IndexWriter.__new__(cls)
    def __init__(self, *args, **kwargs):
        IndexWriter.__init__(self, *args, **kwargs)
        self.indexSearcher = IndexSearcher(self.directory, self.getAnalyzer)
    def __getattr__(self, name):
        if name == 'indexSearcher':
            raise AttributeError(name)
        return getattr(self.indexSearcher, name)
    def __contains__(self, id):
        return id in self.indexSearcher
    def __iter__(self):
        return iter(self.indexSearcher)
    def __getitem__(self, id):
        return self.indexSearcher[id]
    def commit(self):
        "Commit writes and refresh searcher.  Not thread-safe."
        IndexWriter.commit(self)
        if not self.current:
            self.indexSearcher = IndexSearcher(self.directory, self.getAnalyzer)
