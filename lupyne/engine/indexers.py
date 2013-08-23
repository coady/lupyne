"""
Wrappers for lucene Index{Read,Search,Writ}ers.

The final `Indexer`_ classes exposes a high-level Searcher and Writer.
"""

from future_builtins import filter, map, zip
import os
import itertools, operator
import contextlib
import abc, collections
import warnings
import lucene
try:
    from java.io import File, StringReader
    from java.lang import Float
    from java.util import Arrays, HashMap, HashSet
    from org.apache.lucene import analysis, document, index, search, store, util
    from org.apache.lucene.analysis import standard, tokenattributes
    from org.apache.lucene.index import memory
    from org.apache.lucene.search import spans
    from org.apache.pylucene.analysis import PythonAnalyzer, PythonTokenFilter
    from org.apache.lucene.queryparser import classic as queryParser
    from org.apache.lucene.queries import mlt as similar
    from org.apache.pylucene.queryparser.classic import PythonQueryParser
except ImportError:
    from lucene import File, StringReader, Float, Arrays, HashMap, HashSet, PythonAnalyzer, PythonTokenFilter, PythonQueryParser
    analysis = document = index = queryParser = search = store = util = \
    standard = tokenattributes = memory = similar = spans = lucene
from .queries import Query, BooleanFilter, TermsFilter, SortField, Highlighter, FastVectorHighlighter, SpellChecker, SpellParser
from .documents import Field, Document, Hits, Grouping
from .spatial import DistanceComparator

class Atomic(object):
    "Abstract base class to distinguish singleton values from other iterables."
    __metaclass__ = abc.ABCMeta
    @classmethod
    def __subclasshook__(cls, other):
        return not issubclass(other, collections.Iterable) or NotImplemented
for cls in (basestring, analysis.TokenStream, lucene.JArray_byte):
    Atomic.register(cls)

class closing(set):
    "Manage lifespan of registered objects, similar to contextlib.closing."
    def __del__(self):
        for obj in self:
            obj.close()
    def analyzer(self, analyzer, version=None):
        if analyzer is None:
            analyzer = standard.StandardAnalyzer(version or util.Version.values()[-1])
            self.add(analyzer)
        return analyzer
    def directory(self, directory):
        if directory is None:
            directory = store.RAMDirectory()
            self.add(directory)
        elif isinstance(directory, basestring):
            directory = store.FSDirectory.open(File(directory))
            self.add(directory)
        return directory
    def reader(self, reader):
        reader = self.directory(reader)
        if isinstance(reader, index.IndexReader):
            reader.incRef()
        elif isinstance(reader, index.IndexWriter):
            reader = index.IndexReader.open(reader, True)
        else:
            reader = index.IndexReader.open(reader)
        return reader

def copy(commit, dest):
    """Copy the index commit to the destination directory.
    Optimized to use hard links if the destination is a file system path.
    """
    if isinstance(dest, store.Directory):
        args = [store.IOContext.DEFAULT] if hasattr(store, 'IOContext') else []
        for filename in commit.fileNames:
            commit.directory.copy(dest, filename, filename, *args)
    else:
        src = IndexSearcher.path.fget(commit)
        os.path.isdir(dest) or os.makedirs(dest)
        for filename in commit.fileNames:
            paths = os.path.join(src, filename), os.path.join(dest, filename)
            try:
                os.link(*paths)
            except OSError:
                if not os.path.samefile(*paths):
                    raise

class TokenStream(analysis.TokenStream):
    "TokenStream mixin with support for iteration and attributes cached as properties."
    bytes = lucene.VERSION >= '4'
    def __iter__(self):
        return self
    def next(self):
        if self.incrementToken():
            return self
        raise StopIteration
    def __getattr__(self, name):
        cls = getattr(tokenattributes, name + 'Attribute').class_
        attr = self.getAttribute(cls) if self.hasAttribute(cls) else self.addAttribute(cls)
        setattr(self, name, attr)
        return attr
    @property
    def offset(self):
        "Start and stop character offset."
        return self.Offset.startOffset(), self.Offset.endOffset()
    @offset.setter
    def offset(self, item):
        self.Offset.setOffset(*item)
    @property
    def payload(self):
        "Payload bytes."
        payload = self.Payload.payload
        return payload and (payload.utf8ToString() if self.bytes else getattr(payload.data, 'string_', None))
    @payload.setter
    def payload(self, data):
        data = lucene.JArray_byte(data.encode('utf8') if isinstance(data, unicode) else data)
        self.Payload.payload = (util.BytesRef if self.bytes else index.Payload)(data)
    @property
    def positionIncrement(self):
        "Position relative to the previous token."
        return self.PositionIncrement.positionIncrement
    @positionIncrement.setter
    def positionIncrement(self, index):
        self.PositionIncrement.positionIncrement = index
    @property
    def term(self):
        "Term text."
        return self.CharTerm.toString() if self.bytes else self.Term.term()
    @term.setter
    def term(self, text):
        if self.bytes:
            self.CharTerm.setEmpty()
            self.CharTerm.append(text)
        else:
            self.Term.setTermBuffer(text)
    @property
    def type(self):
        "Lexical type."
        return self.Type.type()
    @type.setter
    def type(self, text):
        self.Type.setType(text)

class TokenFilter(PythonTokenFilter, TokenStream):
    """Create an iterable lucene TokenFilter from a TokenStream.
    Subclass and override :meth:`incrementToken` or :meth:`setattrs`.
    """
    def __init__(self, input):
        PythonTokenFilter.__init__(self, input)
        self.input = input
    def incrementToken(self):
        "Advance to next token and return whether the stream is not empty."
        result = self.input.incrementToken()
        self.setattrs()
        return result
    def setattrs(self):
        "Customize current token."

class Analyzer(PythonAnalyzer):
    """Return a lucene Analyzer which chains together a tokenizer and filters.
    
    :param tokenizer: lucene Analyzer or Tokenizer factory
    :param filters: lucene TokenFilters
    """
    def __init__(self, tokenizer, *filters):
        PythonAnalyzer.__init__(self)
        self.tokenizer, self.filters = tokenizer, filters
    def components(self, field, reader):
        source = tokens = self.tokenizer.tokenStream(field, reader) if isinstance(self.tokenizer, analysis.Analyzer) else self.tokenizer(reader)
        for filter in self.filters:
            tokens = filter(tokens)
        tokens.reset()
        return source, tokens
    def tokenStream(self, field, reader):
        return self.components(field, reader)[1]
    def createComponents(self, field, reader):
        return analysis.Analyzer.TokenStreamComponents(*self.components(field, reader))
    def tokens(self, text, field=None):
        "Return lucene TokenStream from text."
        return self.tokenStream(field, StringReader(text))
    def parse(self, query, field='', op='', version='', parser=None, **attrs):
        """Return parsed lucene Query.
        
        :param query: query string
        :param field: default query field name, sequence of names, or boost mapping
        :param op: default query operator ('or', 'and')
        :param version: lucene Version string
        :param parser: custom PythonQueryParser class
        :param attrs: additional attributes to set on the parser
        """
        # parsers aren't thread-safe (nor slow), so create one each time
        args = [util.Version.valueOf('LUCENE_' + version.replace('.', '')) if version else util.Version.values()[-1]]
        if isinstance(field, collections.Mapping):
            boosts = HashMap()
            for key in field:
                boosts.put(key, Float(field[key]))
            args += list(field), self, boosts
        else:
            args += field, self
        parser = (parser or queryParser.QueryParser if isinstance(field, basestring) else queryParser.MultiFieldQueryParser)(*args)
        if op:
            parser.defaultOperator = getattr(queryParser.QueryParser.Operator, op.upper())
        for name, value in attrs.items():
            setattr(parser, name, value)
        if isinstance(parser, queryParser.MultiFieldQueryParser):
            return parser.parse(parser, query)
        try:
            return parser.parse(query)
        finally:
            if isinstance(parser, PythonQueryParser):
                parser.finalize()

class IndexReader(object):
    """Delegated lucene IndexReader, with a mapping interface of ids to document objects.
    
    :param reader: lucene IndexReader
    """
    def __init__(self, reader):
        self.indexReader = reader
    def __getattr__(self, name):
        if name == 'indexReader':
            raise AttributeError(name)
        cls = getattr(index, 'DirectoryReader', index.IndexReader)
        return getattr(cls.cast_(self.indexReader), name)
    def __len__(self):
        return self.numDocs()
    def __contains__(self, id):
        if 0 <= id < self.maxDoc():
            if hasattr(self, 'isDeleted'):
                return not self.isDeleted(id)
            bits = index.MultiFields.getLiveDocs(self.indexReader)
            return bits is None or bits.get(id)
        return False
    def __iter__(self):
        ids = xrange(self.maxDoc())
        if not self.hasDeletions():
            return iter(ids)
        if hasattr(self, 'isDeleted'):
            return itertools.ifilterfalse(self.isDeleted, ids)
        return filter(index.MultiFields.getLiveDocs(self.indexReader).get, ids)
    def __getitem__(self, id):
        return Document(self.document(id))
    @property
    def directory(self):
        "reader's lucene Directory"
        return self.__getattr__('directory')()
    @property
    def path(self):
        "FSDirectory path"
        return store.FSDirectory.cast_(self.directory).directory.path
    @property
    def timestamp(self):
        "timestamp of reader's last commit"
        commit = self.indexCommit
        try:
            modified = commit.timestamp
        except AttributeError:
            modified = store.FSDirectory.fileModified(store.FSDirectory.cast_(commit.directory).directory, commit.segmentsFileName)
        return modified * 0.001
    @property
    def readers(self):
        "segment readers"
        readers = (context.reader() for context in self.leaves()) if hasattr(self, 'leaves') else self.sequentialSubReaders
        return map(index.SegmentReader.cast_, readers)
    @property
    def segments(self):
        "segment filenames with document counts"
        return dict((reader.segmentName, reader.numDocs()) for reader in self.readers)
    def copy(self, dest, query=None, exclude=None, merge=0):
        """Copy the index to the destination directory.
        Optimized to use hard links if the destination is a file system path.
        
        :param dest: destination directory path or lucene Directory
        :param query: optional lucene Query to select documents
        :param exclude: optional lucene Query to exclude documents
        :param merge: optionally merge into maximum number of segments
        """
        copy(self.indexCommit, dest)
        with contextlib.closing(IndexWriter(dest)) as writer:
            if query:
                writer.delete(Query(search.MatchAllDocsQuery) - query)
            if exclude:
                writer.delete(exclude)
            writer.commit()
            writer.forceMergeDeletes()
            if merge:
                writer.forceMerge(merge)
            return len(writer)
    def count(self, name, value):
        "Return number of documents with given term."
        return self.docFreq(index.Term(name, value))
    def names(self, option='all', **attrs):
        """Return field names, given option description.
        
        .. versionchanged:: 1.2 lucene 3.6 requires FieldInfo filter attributes instead of option
        """
        if hasattr(index.IndexReader, 'getFieldNames'):
            return list(self.getFieldNames(getattr(self.FieldOption, option.upper())))
        module = index.MultiFields if hasattr(index, 'MultiFields') else util.ReaderUtil
        fieldinfos = module.getMergedFieldInfos(self.indexReader).iterator()
        return [fieldinfo.name for fieldinfo in fieldinfos if all(getattr(fieldinfo, name) == attrs[name] for name in attrs)]
    def terms(self, name, value='', stop=None, counts=False, **fuzzy):
        """Generate a slice of term values, optionally with frequency counts.
        Supports a range of terms, wildcard terms, or fuzzy terms.
        
        :param name: field name
        :param value: initial term text or wildcard
        :param stop: optional upper bound for simple terms
        :param counts: include frequency counts
        :param fuzzy: optional keyword arguments for fuzzy terms
        """
        term = index.Term(name, value)
        args = fuzzy.get('minSimilarity', 0.5), fuzzy.get('prefixLength', 0)
        if hasattr(index, 'MultiFields'):
            terms = index.MultiFields.getTerms(self.indexReader, name)
            if terms:
                if fuzzy:
                    termenum = search.FuzzyTermsEnum(terms, util.AttributeSource(), term, args[0], args[1], False)
                elif value.endswith('*'): 
                    termenum = search.PrefixTermsEnum(terms.iterator(None), util.BytesRef(value.rstrip('*')))
                else:
                    termenum = search.TermRangeTermsEnum(terms.iterator(None), util.BytesRef(value), stop and util.BytesRef(stop), True, False)
                for bytesref in util.BytesRefIterator.cast_(termenum):
                    text = bytesref.utf8ToString()
                    yield (text, termenum.docFreq()) if counts else text
        else:
            if fuzzy:
                termenum = search.FuzzyTermEnum(self.indexReader, term, *args, **fuzzy)
            elif '*' in value or '?' in value:
                value = value.rstrip('*')
                if '*' in value or '?' in value:
                    warnings.warn('Wildcard term enumeration has been removed from lucene 4; use a prefix instead.', DeprecationWarning)
                termenum = search.WildcardTermEnum(self.indexReader, term)
            else:
                termenum = search.TermRangeTermEnum(self.indexReader, name, value, stop, True, False, None)
            with contextlib.closing(termenum):
                term = termenum.term()
                while term:
                    text = term.text()
                    yield (text, termenum.docFreq()) if counts else text
                    term = termenum.next() and termenum.term()
    def numbers(self, name, step=0, type=int, counts=False):
        """Generate decoded numeric term values, optionally with frequency counts.
        
        :param name: field name
        :param step: precision step to select terms
        :param type: int or float
        :param counts: include frequency counts
        """
        term = index.Term(name, chr(ord(' ') + step))
        decode = util.NumericUtils.prefixCodedToLong
        convert = util.NumericUtils.sortableLongToDouble if issubclass(type, float) else int
        if hasattr(index, 'MultiFields'):
            terms = index.MultiFields.getTerms(self.indexReader, name)
            termenum = search.PrefixTermsEnum(terms.iterator(None), util.BytesRef(term.text()))
            for bytesref in util.BytesRefIterator.cast_(termenum):
                value = convert(decode(bytesref))
                yield (value, termenum.docFreq()) if counts else value
        else:
            with contextlib.closing(search.PrefixTermEnum(self.indexReader, term)) as termenum:
                term = termenum.term()
                while term:
                    value = convert(decode(term.text()))
                    yield (value, termenum.docFreq()) if counts else value
                    term = termenum.next() and termenum.term()
    def docs(self, name, value, counts=False):
        "Generate doc ids which contain given term, optionally with frequency counts."
        if hasattr(index, 'MultiFields'):
            docsenum = index.MultiFields.getTermDocsEnum(self.indexReader, index.MultiFields.getLiveDocs(self.indexReader), name, util.BytesRef(value))
            if docsenum:
                for doc in iter(docsenum.nextDoc, index.DocsEnum.NO_MORE_DOCS):
                    yield (doc, docsenum.freq()) if counts else doc
        else:
            with contextlib.closing(self.termDocs(index.Term(name, value))) as termdocs:
                while termdocs.next():
                    doc = termdocs.doc()
                    yield (doc, termdocs.freq()) if counts else doc
    def positions(self, name, value, payloads=False):
        "Generate doc ids and positions which contain given term, optionally only with payloads."
        if hasattr(index, 'MultiFields'):
            docsenum = index.MultiFields.getTermPositionsEnum(self.indexReader, index.MultiFields.getLiveDocs(self.indexReader), name, util.BytesRef(value))
            if docsenum:
                for doc in iter(docsenum.nextDoc, index.DocsEnum.NO_MORE_DOCS):
                    positions = (docsenum.nextPosition() for n in xrange(docsenum.freq()))
                    if payloads:
                        yield doc, [(position, docsenum.payload.utf8ToString()) for position in positions if docsenum.payload]
                    else:
                        yield doc, list(positions)
        else:
            array = lucene.JArray_byte('')
            with contextlib.closing(self.termPositions(index.Term(name, value))) as termpositions:
                while termpositions.next():
                    doc = termpositions.doc()
                    positions = (termpositions.nextPosition() for n in xrange(termpositions.freq()))
                    if payloads:
                        yield doc, [(position, termpositions.getPayload(array, 0).string_) for position in positions if termpositions.payloadAvailable]
                    else:
                        yield doc, list(positions)
    def comparator(self, name, type='string', parser=None):
        """Return cache of field values suitable for sorting.
        Parsing values into an array is memory optimized.
        Map values into a list for speed optimization.
        
        :param name: field name
        :param type: type object or name compatible with FieldCache
        :param parser: lucene FieldCache.Parser or callable applied to field values
        """
        return SortField(name, type, parser).comparator(self)
    def spans(self, query, positions=False, payloads=False):
        """Generate docs with occurrence counts for a span query.
        
        :param query: lucene SpanQuery
        :param positions: optionally include slice positions instead of counts
        :param payloads: optionally only include slice positions with payloads
        """
        offset = 0
        for reader in self.readers:
            spans_ = query.getSpans(reader.context, reader.liveDocs, HashMap()) if hasattr(reader, 'context') else query.getSpans(reader)
            for doc, spans_ in itertools.groupby(itertools.takewhile(spans.Spans.next, itertools.repeat(spans_)), key=spans.Spans.doc):
                doc += offset
                if payloads:
                    yield doc, [(span.start(), span.end(), [lucene.JArray_byte.cast_(data).string_ for data in span.payload]) \
                        for span in spans_ if span.payloadAvailable]
                elif positions:
                    yield doc, [(span.start(), span.end()) for span in spans_]
                else:
                    yield doc, sum(1 for span in spans_)
            offset += reader.maxDoc()
    def termvector(self, id, field, counts=False):
        "Generate terms for given doc id and field, optionally with frequency counts."
        if hasattr(index.IndexReader, 'getTermFreqVector'):
            tfv = self.getTermFreqVector(id, field) or search.QueryTermVector([])
            for item in zip(tfv.terms, tfv.termFrequencies) if counts else tfv.terms:
                yield item
        else:
            terms = self.getTermVector(id, field)
            if terms:
                termenum = terms.iterator(None)
                for bytesref in util.BytesRefIterator.cast_(termenum):
                    term = bytesref.utf8ToString()
                    yield (term, termenum.totalTermFreq()) if counts else term
    def positionvector(self, id, field, offsets=False):
        "Generate terms and positions for given doc id and field, optionally with character offsets."
        if hasattr(index.IndexReader, 'getTermFreqVector'):
            tpv = index.TermPositionVector.cast_(self.getTermFreqVector(id, field))
            for idx, term in enumerate(tpv.terms):
                if offsets:
                    yield term, list(map(operator.attrgetter('startOffset', 'endOffset'), tpv.getOffsets(idx)))
                else:
                    yield term, list(tpv.getTermPositions(idx))
        else:
            termenum = self.getTermVector(id, field).iterator(None)
            for bytesref in util.BytesRefIterator.cast_(termenum):
                term = bytesref.utf8ToString()
                docsenum = termenum.docsAndPositions(None, None)
                assert 0 <= docsenum.nextDoc() < docsenum.NO_MORE_DOCS
                positions = (docsenum.nextPosition() for n in xrange(docsenum.freq()))
                if offsets:
                    yield term, [(docsenum.startOffset(), docsenum.endOffset()) for position in positions]
                else:
                    yield term, list(positions)
    def morelikethis(self, doc, *fields, **attrs):
        """Return MoreLikeThis query for document.
        
        :param doc: document id or text
        :param fields: document fields to use, optional for termvectors
        :param attrs: additional attributes to set on the morelikethis object
        """
        mlt = similar.MoreLikeThis(self.indexReader)
        mlt.fieldNames = fields or None
        for name, value in attrs.items():
            setattr(mlt, name, value)
        return mlt.like(StringReader(doc), '') if isinstance(doc, basestring) else mlt.like(doc)

class IndexSearcher(search.IndexSearcher, IndexReader):
    """Inherited lucene IndexSearcher, with a mixed-in IndexReader.
    
    :param directory: directory path, lucene Directory, or lucene IndexReader
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    """
    def __init__(self, directory, analyzer=None):
        self.shared = closing()
        search.IndexSearcher.__init__(self, self.shared.reader(directory))
        self.analyzer = self.shared.analyzer(analyzer)
        self.filters, self.sorters, self.spellcheckers = {}, {}, {}
        self.termsfilters, self.groupings = set(), {}
    @classmethod
    def load(cls, directory, analyzer=None):
        "Open `IndexSearcher`_ with a lucene RAMDirectory, loading index into memory."
        ref = closing()
        directory = ref.directory(directory)
        try:
            directory = store.RAMDirectory(directory)
        except lucene.InvalidArgsError:
            directory = store.RAMDirectory(directory, store.IOContext.DEFAULT)
        self = cls(directory, analyzer)
        self.shared.add(self.directory)
        return self
    def __del__(self):
        if hash(self):
            self.decRef()
    def reopen(self, filters=False, sorters=False, spellcheckers=False):
        """Return current `IndexSearcher`_, only creating a new one if necessary.
        Any registered :attr:`termsfilters` are also refreshed.
        
        :param filters: refresh cached facet :attr:`filters`
        :param sorters: refresh cached :attr:`sorters` with associated parsers
        :param spellcheckers: refresh cached :attr:`spellcheckers`
        """
        cls = getattr(index, 'DirectoryReader', index.IndexReader)
        try:
            reader = cls.openIfChanged(cls.cast_(self.indexReader))
        except TypeError:
            readers = [cls.openIfChanged(cls.cast_(reader)) for reader in self.sequentialSubReaders]
            reader = index.MultiReader([new or old for new, old in zip(readers, self.sequentialSubReaders)]) if any(readers) else None
        if reader is None:
            return self
        other = type(self)(reader, self.analyzer)
        other.decRef()
        other.shared = self.shared
        other.filters.update((key, value if isinstance(value, search.Filter) else dict(value)) for key, value in self.filters.items())
        for termsfilter in self.termsfilters:
            termsfilter.refresh(other)
        if filters:
            other.facets(Query.any(), *other.filters)
        other.sorters = dict((name, SortField(sorter.field, sorter.typename, sorter.parser)) for name, sorter in self.sorters.items())
        if sorters:
            for field in self.sorters:
                other.comparator(field)
        if spellcheckers:
            for field in self.spellcheckers:
                other.spellchecker(field)
        else:
            other.spellcheckers = dict(self.spellcheckers)
        return other
    def __getitem__(self, id):
        return Document(self.doc(id))
    def get(self, id, *fields):
        "Return `Document`_ with only selected fields loaded."
        return Document(self.document(id, getattr(document, 'MapFieldSelector', HashSet)(Arrays.asList(fields))))
    def parse(self, query, spellcheck=False, **kwargs):
        if isinstance(query, search.Query):
            return query
        if spellcheck:
            kwargs['parser'], kwargs['searcher'] = SpellParser, self
        return Analyzer.__dict__['parse'](self.analyzer, query, **kwargs)
    def highlighter(self, query, field, **kwargs):
        "Return `Highlighter`_ or if applicable `FastVectorHighlighter`_ specific to searcher and query."
        query = self.parse(query, field=field)
        if hasattr(index, 'MultiFields'):
            fieldinfo = index.MultiFields.getMergedFieldInfos(self.indexReader).fieldInfo(field)
            vector = fieldinfo and fieldinfo.hasVectors()
        else:
            vector = field in self.names('termvector_with_position_offset', storeTermVector=True)
        return (FastVectorHighlighter if vector else Highlighter)(self, query, field, **kwargs)
    def count(self, *query, **options):
        """Return number of hits for given query or term.
        
        :param query: :meth:`search` compatible query, or optimally a name and value
        :param options: additional :meth:`search` options
        """
        if len(query) > 1:
            return self.docFreq(index.Term(*query))
        query = self.parse(*query, **options) if query else search.MatchAllDocsQuery()
        collector = search.TotalHitCountCollector()
        search.IndexSearcher.search(self, query, options.get('filter'), collector)
        return collector.totalHits
    def collector(self, query, count=None, sort=None, reverse=False, scores=False, maxscore=False):
        inorder = not self.createNormalizedWeight(query).scoresDocsOutOfOrder()
        if count is None:
            return search.CachingCollector.create(not inorder, True, float('inf'))
        count = min(count, self.maxDoc() or 1)
        if sort is None:
            return search.TopScoreDocCollector.create(count, inorder)
        if isinstance(sort, basestring):
            sort = self.sorter(sort, reverse=reverse)
        if not isinstance(sort, search.Sort):
            sort = search.Sort(sort)
        return search.TopFieldCollector.create(sort, count, True, scores, maxscore, inorder)
    def search(self, query=None, filter=None, count=None, sort=None, reverse=False, scores=False, maxscore=False, timeout=None, **parser):
        """Run query and return `Hits`_.
        
        .. versionchanged:: 1.4 sort param for lucene only;  use Hits.sorted with a callable
        
        :param query: query string or lucene Query
        :param filter: lucene Filter
        :param count: maximum number of hits to retrieve
        :param sort: lucene Sort parameters
        :param reverse: reverse flag used with sort
        :param scores: compute scores for candidate results when sorting
        :param maxscore: compute maximum score of all results when sorting
        :param timeout: stop search after elapsed number of seconds
        :param parser: :meth:`Analyzer.parse` options
        """
        query = search.MatchAllDocsQuery() if query is None else self.parse(query, **parser)
        cache = collector = self.collector(query, count, sort, reverse, scores, maxscore)
        counter = search.TimeLimitingCollector.getGlobalCounter()
        results = collector if timeout is None else search.TimeLimitingCollector(collector, counter, long(timeout * 1000))
        try:
            search.IndexSearcher.search(self, query, filter, results)
        except lucene.JavaError as timeout:
            if not search.TimeLimitingCollector.TimeExceededException.instance_(timeout.getJavaException()):
                raise
        if isinstance(cache, search.CachingCollector):
            collector = search.TotalHitCountCollector()
            cache.replay(collector)
            collector = self.collector(query, collector.totalHits or 1, sort, reverse, scores, maxscore)
            cache.replay(collector)
        topdocs = collector.topDocs()
        stats = (topdocs.totalHits, topdocs.maxScore) * (not isinstance(timeout, lucene.JavaError))
        return Hits(self, topdocs.scoreDocs, *stats)
    def facets(self, query, *keys):
        """Return mapping of document counts for the intersection with each facet.
        
        :param query: query string, lucene Query, or lucene Filter
        :param keys: field names, term tuples, or any keys to previously cached filters
        """
        counts = collections.defaultdict(dict)
        if isinstance(query, basestring):
            query = self.parse(query)
        if isinstance(query, search.Query):
            query = search.QueryWrapperFilter(query)
        if not isinstance(query, search.CachingWrapperFilter):
            query = search.CachingWrapperFilter(query)
        for key in keys:
            filters = self.filters.get(key)
            if key in self.groupings:
                counts[key] = dict(self.groupings[key].facets(query))
            elif isinstance(filters, search.Filter):
                counts[key] = self.count(filter=BooleanFilter.all(query, filters))
            else:
                name, value = (key, None) if isinstance(key, basestring) else key
                filters = self.filters.setdefault(name, {})
                if value is None:
                    values = filters or self.terms(name)
                else:
                    values = [value] if value in filters else self.terms(name, value)
                for value in values:
                    if value not in filters:
                        filters[value] = Query.term(name, value).filter()
                    counts[name][value] = self.count(filter=BooleanFilter.all(query, filters[value]))
        return dict(counts)
    def grouping(self, field, query=None, count=None, sort=None):
        "Return `Grouping`_ for unique field and lucene search parameters."
        try:
            return self.groupings[field]
        except KeyError:
            return Grouping(self, field, query, count, sort)
    def sorter(self, field, type='string', parser=None, reverse=False):
        "Return `SortField`_ with cached attributes if available."
        sorter = self.sorters.get(field, SortField(field, type, parser, reverse))
        return sorter if sorter.reverse == reverse else SortField(sorter.field, sorter.typename, sorter.parser, reverse)
    def comparator(self, field, type='string', parser=None):
        "Return :meth:`IndexReader.comparator` using a cached `SortField`_ if available."
        return self.sorter(field, type, parser).comparator(self)
    def distances(self, lng, lat, lngfield, latfield):
        "Return distance comparator computed from cached lat/lng fields."
        arrays = (self.comparator(field, 'double') for field in (lngfield, latfield))
        return DistanceComparator(lng, lat, *arrays)
    def spellchecker(self, field):
        "Return and cache spellchecker for given field."
        try:
            return self.spellcheckers[field]
        except KeyError:
            return self.spellcheckers.setdefault(field, SpellChecker(self.terms(field, counts=True)))
    def suggest(self, field, prefix, count=None):
        "Return ordered suggested words for prefix."
        return self.spellchecker(field).suggest(prefix, count)
    def correct(self, field, text, distance=2, minSimilarity=0.5):
        """Generate potential words ordered by increasing edit distance and decreasing frequency.
        For optimal performance only iterate the required slice size of corrections.
        
        :param distance: the maximum edit distance to consider for enumeration
        :param minSimilarity: threshold for additional fuzzy terms after edits have been exhausted
        """
        spellchecker = self.spellchecker(field)
        corrections = set()
        for words in itertools.islice(spellchecker.correct(text), distance + 1):
            for word in words:
                yield word
            corrections.update(words)
        if minSimilarity:
            words = set(self.terms(field, text, minSimilarity=minSimilarity)) - corrections
            for word in sorted(words, key=spellchecker.__getitem__, reverse=True):
                yield word
    def match(self, document, *queries):
        "Generate scores for all queries against a given document mapping."
        searcher = memory.MemoryIndex()
        for name, value in document.items():
            if isinstance(value, basestring):
                value = value, self.analyzer
            elif isinstance(value, analysis.TokenStream):
                value = value,
            searcher.addField(name, *value)
        for query in queries:
            yield searcher.search(self.parse(query))

class MultiSearcher(IndexSearcher):
    """IndexSearcher with underlying lucene MultiReader.
    
    :param reader: directory paths, Directories, IndexReaders, or a single MultiReader
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    """
    def __init__(self, reader, analyzer=None):
        shared = closing()
        if not index.MultiReader.instance_(reader):
            reader = index.MultiReader(list(map(shared.reader, reader)))
            ref = closing([reader])
        IndexSearcher.__init__(self, reader, analyzer)
        self.shared.update(shared)
        shared.clear()
        if not hasattr(self, 'sequentialSubReaders'):
            self.sequentialSubReaders = [context.reader() for context in self.context.children()]
        self.version = sum(IndexReader(reader).version for reader in self.sequentialSubReaders)
    def __getattr__(self, name):
        return getattr(index.MultiReader.cast_(self.indexReader), name)
    @property
    def readers(self):
        return itertools.chain.from_iterable(IndexReader(reader).readers for reader in self.sequentialSubReaders)
    @property
    def timestamp(self):
        return max(IndexReader(reader).timestamp for reader in self.sequentialSubReaders)

class IndexWriter(index.IndexWriter):
    """Inherited lucene IndexWriter.
    Supports setting fields parameters explicitly, so documents can be represented as dictionaries.
    
    :param directory: directory path or lucene Directory, default RAMDirectory
    :param mode: file mode (rwa), except updating (+) is implied
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    :param version: lucene Version argument passed to IndexWriterConfig or StandardAnalyzer, default is latest
    :param attrs: additional attributes to set on IndexWriterConfig
    """
    __len__ = index.IndexWriter.numDocs
    parse = IndexSearcher.__dict__['parse']
    def __init__(self, directory=None, mode='a', analyzer=None, version=None, **attrs):
        self.shared = closing()
        if version is None:
            version = util.Version.values()[-1]
        config = index.IndexWriterConfig(version, self.shared.analyzer(analyzer, version))
        config.openMode = index.IndexWriterConfig.OpenMode.values()['wra'.index(mode)]
        for name, value in attrs.items():
            setattr(config, name, value)
        config.indexDeletionPolicy = index.SnapshotDeletionPolicy(config.indexDeletionPolicy)
        index.IndexWriter.__init__(self, self.shared.directory(directory), config)
        self.policy = index.SnapshotDeletionPolicy.cast_(self.config.indexDeletionPolicy)
        self.fields = {}
    def __del__(self):
        if hash(self):
            self.close()
    def set(self, name, cls=Field, **params):
        """Assign parameters to field name.
        
        :param name: registered name of field
        :param cls: optional `Field`_ constructor
        :param params: store,index,termvector options compatible with `Field`_
        """
        self.fields[name] = cls(name, **params)
    def document(self, items=(), **terms):
        "Return lucene Document from mapping of field names to one or multiple values."
        doc = document.Document()
        for name, values in dict(items, **terms).items():
            if isinstance(values, Atomic):
                values = values,
            for field in self.fields[name].items(*values):
                doc.add(field)
        return doc
    def add(self, document=(), **terms):
        "Add :meth:`document` to index with optional boost."
        self.addDocument(self.document(document, **terms))
    def update(self, name, value='', document=(), **terms):
        "Atomically delete documents which match given term and add the new :meth:`document` with optional boost."
        doc = self.document(document, **terms)
        self.updateDocument(index.Term(name, *[value] if value else doc.getValues(name)), doc)
    def delete(self, *query, **options):
        """Remove documents which match given query or term.
        
        :param query: :meth:`IndexSearcher.search` compatible query, or optimally a name and value
        :param options: additional :meth:`Analyzer.parse` options
        """
        parse = self.parse if len(query) == 1 else index.Term
        self.deleteDocuments(parse(*query, **options))
    def __iadd__(self, directory):
        "Add directory (or reader, searcher, writer) to index."
        ref = closing()
        directory = ref.directory(directory)
        self.addIndexes([directory if isinstance(directory, store.Directory) else directory.directory])
        return self
    @contextlib.contextmanager
    def snapshot(self, id=''):
        """Return context manager of an index commit snapshot.
        
        .. versionchanged:: 1.4 lucene 4.4 identifies snapshots by commit generation
        """
        commit = self.policy.snapshot(id) if lucene.VERSION < '4.4' else self.policy.snapshot()
        try:
            yield commit
        finally:
            self.policy.release(id if lucene.VERSION < '4.4' else commit)

class Indexer(IndexWriter):
    """An all-purpose interface to an index.
    Creates an `IndexWriter`_ with a delegated `IndexSearcher`_.
    
    :param nrt: optionally use a near real-time searcher
    """
    def __init__(self, directory=None, mode='a', analyzer=None, version=None, nrt=False, **attrs):
        IndexWriter.__init__(self, directory, mode, analyzer, version, **attrs)
        IndexWriter.commit(self)
        self.nrt = nrt
        self.indexSearcher = IndexSearcher(self if nrt else self.directory, self.analyzer)
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
    def refresh(self, **caches):
        "Store refreshed searcher with :meth:`IndexSearcher.reopen` caches."
        self.indexSearcher = self.indexSearcher.reopen(**caches)
    def commit(self, merge=False, **caches):
        """Commit writes and :meth:`refresh` searcher.
        
        :param merge: merge segments with deletes, or optionally specify maximum number of segments
        """
        IndexWriter.commit(self)
        if merge:
            if isinstance(merge, bool):
                self.forceMergeDeletes()
            else:
                self.forceMerge(merge)
            IndexWriter.commit(self)
        self.refresh(**caches)

class ParallelIndexer(Indexer):
    """Indexer which tracks a unique identifying field.
    Handles atomic updates of rapidly changing fields, managing :attr:`termsfilters`.
    """
    def __init__(self, field, *args, **kwargs):
        Indexer.__init__(self, *args, **kwargs)
        self.field = field
        self.set(field, index=True, omitNorms=True)
        self.termsfilters = {}
    def termsfilter(self, filter, *others):
        "Return `TermsFilter`_ synced to given filter and optionally associated with other indexers."
        terms = self.sorter(self.field).terms(filter, *self.readers)
        termsfilter = self.termsfilters[filter] = TermsFilter(self.field, terms)
        for other in others:
            termsfilter.refresh(other)
            other.termsfilters.add(termsfilter)
        return termsfilter
    def update(self, value, document=(), **terms):
        "Atomically update document based on unique field."
        terms[self.field] = value
        self.updateDocument(index.Term(self.field, value), self.document(document, **terms))
    def refresh(self, **caches):
        "Store refreshed searcher and synchronize :attr:`termsfilters`."
        sorter, segments = self.sorter(self.field), self.segments
        searcher = self.indexSearcher.reopen(**caches)
        readers = [reader for reader in searcher.readers if reader.segmentName not in segments]
        terms = list(itertools.chain.from_iterable(IndexReader(reader).terms(self.field) for reader in readers))
        for filter, termsfilter in self.termsfilters.items():
            if terms:
                termsfilter.update(terms, op='andNot', cache=not self.nrt)
            if readers:
                termsfilter.update(sorter.terms(filter, *readers), cache=not self.nrt)
        self.indexSearcher = searcher
