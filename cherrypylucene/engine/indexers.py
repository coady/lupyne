"""
Wrappers for lucene Index{Read,Search,Writ}ers.

The final `Indexer`_ classes exposes a high-level Searcher and Writer.
"""

import itertools, operator
import contextlib
import lucene
from queries import HitCollector, Filter, Query
from documents import Field, Document, Hits

def iterate(jit, positioned=False):
    """Transform java iterator into python iterator.
    
    :positioned: current iterator position is valid
    """
    with contextlib.closing(jit):
        if positioned:
            yield jit
        while jit.next():
            yield jit

class IndexReader(object):
    """Delegated lucene IndexReader, with a mapping interface of ids to document objects.

    :directory: lucene IndexReader or directory.
    """
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
        "reader's lucene Directory"
        return self.indexReader.directory()
    def delete(self, name, value):
        """Delete documents with given term.
        
        Acquires a write lock.  Deleting from an `IndexWriter`_ is encouraged instead.
        """
        self.deleteDocuments(lucene.Term(name, value))
    def count(self, name, value):
        "Return number of documents with given term."
        return self.docFreq(lucene.Term(name, value))
    def names(self, option='all'):
        "Return field names, given option description."
        option = getattr(self.FieldOption, option.upper())
        return list(self.getFieldNames(option))
    def terms(self, name, start='', stop=None, counts=False):
        "Generate a slice of term values, optionally with frequency counts."
        for termenum in iterate(self.indexReader.terms(lucene.Term(name, start)), positioned=True):
            term = termenum.term()
            if term and term.field() == name:
                text = term.text()
                if stop is None or text < stop:
                    yield (text, termenum.docFreq()) if counts else text
                    continue
            break
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

class IndexSearcher(lucene.IndexSearcher, IndexReader):
    """Inherited lucene IndexSearcher, with a delegated IndexReader as well.
    
    :param directory: directory path or lucene Directory
    :param analyzer: lucene Analyzer class
    """
    def __init__(self, directory, analyzer=lucene.StandardAnalyzer):
        lucene.IndexSearcher.__init__(self, directory)
        self.analyzer = analyzer()
    def __del__(self):
        if str(self) != '<null>':
            self.close()
    def parse(self, query, field='', op='or'):
        """Return lucene parsed Query.
        
        :param field: default query field name
        :param op: default query operator
        """
        # parser's aren't thread-safe (nor slow), so create one each time
        parser = lucene.QueryParser(field, self.analyzer)
        parser.defaultOperator = getattr(lucene.QueryParser.Operator, op.upper())
        return parser.parse(query)
    def count(self, *query, **options):
        """Return number of hits for given query or term.
        
        :param query: :meth:`search` compatible query, or optimally a name and value
        :param options: additional :meth:`search` options
        """
        if len(query) == 1:
            return self.search(query[0], count=1, **options).count
        return IndexReader.count(self, *query)
    def search(self, query, filter=None, count=None, sort=None, reverse=False, **parser):
        """Run query and return `Hits`_.
        
        :param query: query string, `Query`_ object, or lucene Query
        :param filter: doc ids or lucene Filter
        :param count: maximum number of hits to retrieve
        :param sort: field name, names, or lucene Sort
        :param reverse: reverse flag used with sort
        :param parser: :meth:`parse` options
        """
        if not isinstance(query, lucene.Query):
            query = query.q if isinstance(query, Query) else self.parse(query, **parser)
        if not isinstance(filter, (lucene.Filter, type(None))):
            filter = Filter(filter)
        # use custom HitCollector if all results are necessary, otherwise let lucene's TopDocs handle it
        if count is None:
            collector = HitCollector(self)
            lucene.IndexSearcher.search(self, query, filter, collector)
            return Hits(self, *collector.sorted(key=sort, reverse=reverse))
        if sort is None:
            topdocs = lucene.IndexSearcher.search(self, query, filter, count)
        else:
            if not isinstance(sort, lucene.Sort):
                sort = lucene.Sort(sort, reverse) if isinstance(sort, basestring) else lucene.Sort(sort)
            topdocs = lucene.IndexSearcher.search(self, query, filter, count, sort)
        ids, scores = (map(operator.attrgetter(name), topdocs.scoreDocs) for name in ['doc', 'score'])
        return Hits(self, ids, scores, topdocs.totalHits)

class IndexWriter(lucene.IndexWriter):
    """Inherited lucene IndexWriter.
    Supports setting fields parameters explicitly, so documents can be represented as dictionaries.

    :param directory: directory path or lucene Directory.
    :param mode: file mode: 'a' creates if necessary, 'w' overwrites, 'r' doesn't create
    :param analyzer: lucene Analyzer class
    """
    __len__ = lucene.IndexWriter.numDocs
    __del__ = IndexSearcher.__del__.im_func
    parse = IndexSearcher.parse.im_func
    def __init__(self, directory=None, mode='a', analyzer=lucene.StandardAnalyzer):
        create = [mode == 'w'] * (mode != 'a')
        lucene.IndexWriter.__init__(self, directory or lucene.RAMDirectory(), analyzer(), *create)
        self.fields = {}
    @property
    def segments(self):
        "segment filenames with document counts"
        items = (seg.split(':c') for seg in self.segString().split())
        return dict((name, int(value)) for name, value in items)
    def set(self, name, cls=Field, **params):
        """Assign parameters to field name.
        
        :param name: registered name of field
        :param cls: optional `Field`_ constructor
        :param params: store,index,termvector options compatible with `Field`_
        """
        self.fields[name] = cls(name, **params)
    def add(self, document=(), **terms):
        """Add document to index.
        Document is comprised of name: value pairs, where the values may be one or multiple strings.
        
        :param document: optional document terms as a dict or items
        :param terms: additional terms to document
        """
        terms.update(document)
        doc = lucene.Document()
        for name, values in terms.items():
            if isinstance(values, basestring):
                values = [values] 
            for field in self.fields[name].items(*values):
                doc.add(field)
        self.addDocument(doc)
    def delete(self, *query, **options):
        """Remove documents which match given query or term.
        
        :param query: :meth:`IndexSearcher.search` compatible query, or optimally a name and value
        :param options: additional :meth:`parse` options
        """
        if len(query) == 1:
            query = query[0]
            if not isinstance(query, lucene.Query):
                query = query.q if isinstance(query, Query) else self.parse(query, **options)
            self.deleteDocuments(query)
        else:
            self.deleteDocuments(lucene.Term(*query))
    def __iadd__(self, directory):
        "Add directory (or reader, searcher, writer) to index."
        if isinstance(directory, basestring):
            directory = lucene.FSDirectory.getDirectory(directory)
        elif not isinstance(directory, lucene.Directory):
            directory = directory.directory
        self.addIndexesNoOptimize([directory])
        return self

class Indexer(IndexWriter):
    """An all-purpose interface to an index.
    Opening in read mode returns an `IndexSearcher`_.
    Opening in write mode (the default) returns an `IndexWriter`_ with a delegated `IndexSearcher`_.
    """
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
