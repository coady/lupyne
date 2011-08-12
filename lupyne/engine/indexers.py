"""
Wrappers for lucene Index{Read,Search,Writ}ers.

The final `Indexer`_ classes exposes a high-level Searcher and Writer.
"""

from future_builtins import map, zip
import os
import re
import itertools, operator
import contextlib
import abc, collections
import lucene
from .queries import Query, Collector, SortField, Highlighter, FastVectorHighlighter, SpellChecker, SpellParser
from .documents import Field, Document, Hits

class Atomic(object):
    "Abstract base class to distinguish singleton values from other iterables."
    __metaclass__ = abc.ABCMeta
    @classmethod
    def __subclasshook__(cls, other):
        return not issubclass(other, collections.Iterable) or NotImplemented
Atomic.register(basestring)
Atomic.register(lucene.TokenStream)

class closing(set):
    "Manage lifespan of registered objects, similar to contextlib.closing."
    def __del__(self):
        for obj in self:
            obj.close()
    def analyzer(self, analyzer, version=None):
        if analyzer is None:
            analyzer = lucene.StandardAnalyzer(version or lucene.Version.values()[-1])
            self.add(analyzer)
        return analyzer
    def directory(self, directory):
        if directory is None:
            directory = lucene.RAMDirectory()
            self.add(directory)
        elif isinstance(directory, basestring):
            directory = lucene.FSDirectory.open(lucene.File(directory))
            self.add(directory)
        return directory
    def reader(self, reader):
        reader = self.directory(reader)
        if isinstance(reader, lucene.IndexReader):
            reader.incRef()
        else:
            reader = lucene.IndexReader.open(reader)
        return reader

class TokenFilter(lucene.PythonTokenFilter):
    """Create an iterable lucene TokenFilter from a TokenStream.
    Subclass and override :meth:`incrementToken`.
    Attributes are cached as properties to create a Token interface.
    """
    def __init__(self, input):
        lucene.PythonTokenFilter.__init__(self, input)
        self.input = input
    def __iter__(self):
        return self
    def next(self):
        if self.incrementToken():
            return self
        raise StopIteration
    def incrementToken(self):
        "Advance to next token and return whether the stream is not empty."
        return self.input.incrementToken()
    def __getattr__(self, name):
        cls = getattr(lucene, name + 'Attribute').class_
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
        return payload and getattr(payload.data, 'string_', None)
    @payload.setter
    def payload(self, data):
        self.Payload.payload = lucene.Payload(lucene.JArray_byte(data))
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
        return self.Term.term()
    @term.setter
    def term(self, text):
        self.Term.setTermBuffer(text)
    @property
    def type(self):
        "Lexical type."
        return self.Type.type()
    @type.setter
    def type(self, text):
        self.Type.setType(text)

class Analyzer(lucene.PythonAnalyzer):
    """Return a lucene Analyzer which chains together a tokenizer and filters.
    
    :param tokenizer: lucene Tokenizer or Analyzer
    :param filters: lucene TokenFilters
    """
    def __init__(self, tokenizer, *filters):
        lucene.PythonAnalyzer.__init__(self)
        self.tokenizer, self.filters = tokenizer, filters
    def tokenStream(self, field, reader):
        tokens = self.tokenizer.tokenStream(field, reader) if isinstance(self.tokenizer, lucene.Analyzer) else self.tokenizer(reader)
        for filter in self.filters:
            tokens = filter(tokens)
        return tokens
    def tokens(self, text, field=None):
        "Return lucene TokenStream from text."
        return self.tokenStream(field, lucene.StringReader(text))
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
        args = [lucene.Version.valueOf('LUCENE_' + version.replace('.', '')) if version else lucene.Version.values()[-1]]
        if isinstance(field, collections.Mapping):
            boosts = lucene.HashMap()
            for key in field:
                boosts.put(key, lucene.Float(field[key]))
            args += list(field), self, boosts
        else:
            args += field, self
        parser = (parser or lucene.QueryParser if isinstance(field, basestring) else lucene.MultiFieldQueryParser)(*args)
        if op:
            parser.defaultOperator = getattr(lucene.QueryParser.Operator, op.upper())
        for name, value in attrs.items():
            setattr(parser, name, value)
        if isinstance(parser, lucene.MultiFieldQueryParser):
            return lucene.MultiFieldQueryParser.parse(parser, query)
        try:
            return parser.parse(query)
        finally:
            if isinstance(parser, lucene.PythonQueryParser):
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
        return getattr(self.indexReader, name)
    def __len__(self):
        return self.numDocs()
    def __contains__(self, id):
        return 0 <= id < self.maxDoc() and not self.isDeleted(id)
    def __iter__(self):
        return itertools.ifilterfalse(self.isDeleted, xrange(self.maxDoc()))
    def __getitem__(self, id):
        return Document(self.document(id))
    @property
    def directory(self):
        "reader's lucene Directory"
        return self.indexReader.directory()
    @property
    def timestamp(self):
        "timestamp of reader's last commit"
        return self.indexCommit.timestamp / 1000.0
    @property
    def segments(self):
        "segment filenames with document counts"
        return dict((lucene.SegmentReader.cast_(reader).segmentName, reader.numDocs()) for reader in self.sequentialSubReaders)
    def copy(self, dest, query=None, exclude=None, optimize=False):
        """Copy the index to the destination directory.
        Optimized to use hard links if the destination is a file system path.
        
        :param dest: destination directory path or lucene Directory
        :param query: optional lucene Query to select documents
        :param exclude: optional lucene Query to exclude documents
        :param optimize: optionally optimize destination index
        """
        filter = lucene.IndexFileNameFilter.getFilter()
        filenames = (filename for filename in self.directory.listAll() if filter.accept(None, filename))
        if isinstance(dest, lucene.Directory):
            src = self.directory
            try:
                lucene.Directory.copy(src, dest, False)
            except lucene.InvalidArgsError:
                for filename in filenames:
                    src.copy(dest, filename, filename)
        else:
            src = lucene.FSDirectory.cast_(self.directory).file.path
            os.path.isdir(dest) or os.makedirs(dest)
            for filename in filenames:
                os.link(os.path.join(src, filename), os.path.join(dest, filename))
        with contextlib.closing(IndexWriter(dest)) as writer:
            if query:
                writer.delete(Query(lucene.MatchAllDocsQuery) - query)
            if exclude:
                writer.delete(exclude)
            writer.commit()
            writer.expungeDeletes()
            if optimize:
                writer.optimize(optimize)
        return len(writer)
    def count(self, name, value):
        "Return number of documents with given term."
        return self.docFreq(lucene.Term(name, value))
    def names(self, option='all'):
        "Return field names, given option description."
        option = getattr(self.FieldOption, option.upper())
        return list(self.getFieldNames(option))
    def terms(self, name, value='', stop=None, counts=False, **fuzzy):
        """Generate a slice of term values, optionally with frequency counts.
        Supports a range of terms, wildcard terms, or fuzzy terms.
        
        :param name: field name
        :param value: initial term text or wildcard
        :param stop: optional upper bound for simple terms
        :param counts: include frequency counts
        :param fuzzy: optional keyword arguments for fuzzy terms
        """
        term = lucene.Term(name, value)
        if fuzzy:
            args = fuzzy.pop('minSimilarity', 0.5), fuzzy.pop('prefixLength', 0)
            termenum = lucene.FuzzyTermEnum(self.indexReader, term, *args, **fuzzy)
        elif '*' in value or '?' in value:
            termenum = lucene.WildcardTermEnum(self.indexReader, term)
        else:
            termenum = lucene.TermRangeTermEnum(self.indexReader, name, value, stop, True, False, None)
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
        term = lucene.Term(name, chr(ord(' ') + step))
        decode = lucene.NumericUtils.prefixCodedToLong
        convert = lucene.NumericUtils.sortableLongToDouble if issubclass(type, float) else int
        with contextlib.closing(lucene.PrefixTermEnum(self.indexReader, term)) as termenum:
            term = termenum.term()
            while term:
                value = convert(decode(term.text()))
                yield (value, termenum.docFreq()) if counts else value
                term = termenum.next() and termenum.term()
    def docs(self, name, value, counts=False):
        "Generate doc ids which contain given term, optionally with frequency counts."
        with contextlib.closing(self.termDocs(lucene.Term(name, value))) as termdocs:
            while termdocs.next():
                doc = termdocs.doc()
                yield (doc, termdocs.freq()) if counts else doc
    def positions(self, name, value, payloads=False):
        "Generate doc ids and positions which contain given term, optionally only with payloads."
        array = lucene.JArray_byte('')
        with contextlib.closing(self.termPositions(lucene.Term(name, value))) as termpositions:
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
        return SortField(name, type, parser).comparator(self.indexReader)
    def spans(self, query, positions=False, payloads=False):
        """Generate docs with occurrence counts for a span query.
        
        :param query: lucene SpanQuery
        :param positions: optionally include slice positions instead of counts
        :param payloads: optionally only include slice positions with payloads
        """
        spans = itertools.takewhile(lucene.Spans.next, itertools.repeat(query.getSpans(self.indexReader)))
        for doc, spans in itertools.groupby(spans, key=lucene.Spans.doc):
            if payloads:
                yield doc, [(span.start(), span.end(), [lucene.JArray_byte.cast_(data).string_ for data in span.payload]) \
                    for span in spans if span.payloadAvailable]
            elif positions:
                yield doc, [(span.start(), span.end()) for span in spans]
            else:
                yield doc, sum(1 for span in spans)
    def termvector(self, id, field, counts=False):
        "Generate terms for given doc id and field, optionally with frequency counts."
        tfv = self.getTermFreqVector(id, field)
        return zip(tfv.terms, tfv.termFrequencies) if counts else iter(tfv.terms)
    def positionvector(self, id, field, offsets=False):
        "Generate terms and positions for given doc id and field, optionally with character offsets."
        tpv = lucene.TermPositionVector.cast_(self.getTermFreqVector(id, field))
        for index, term in enumerate(tpv.terms):
            if offsets:
                yield term, list(map(operator.attrgetter('startOffset', 'endOffset'), tpv.getOffsets(index)))
            else:
                yield term, list(tpv.getTermPositions(index))
    def morelikethis(self, doc, *fields, **attrs):
        """Return MoreLikeThis query for document.
        
        :param doc: document id or text
        :param fields: document fields to use, optional for termvectors
        :param attrs: additional attributes to set on the morelikethis object
        """
        mlt = lucene.MoreLikeThis(self.indexReader)
        mlt.fieldNames = fields or None
        for name, value in attrs.items():
            setattr(mlt, name, value)
        return mlt.like(lucene.StringReader(doc) if isinstance(doc, basestring) else doc)
    def overlap(self, left, right):
        "Return intersection count of cached filters."
        count = 0
        for reader in self.sequentialSubReaders:
            docsets = left.getDocIdSet(reader), right.getDocIdSet(reader)
            try:
                count += lucene.OpenBitSet.intersectionCount(*docsets)
            except lucene.InvalidArgsError: # verify docsets are either cached or empty
                if not all(lucene.OpenBitSet.instance_(docset) or docset.iterator().nextDoc() == lucene.Integer.MAX_VALUE for docset in docsets):
                    raise
        return int(count)

class IndexSearcher(lucene.IndexSearcher, IndexReader):
    """Inherited lucene IndexSearcher, with a mixed-in IndexReader.
    
    :param directory: directory path, lucene Directory, or lucene IndexReader
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    """
    def __init__(self, directory, analyzer=None):
        self.shared = closing()
        lucene.IndexSearcher.__init__(self, self.shared.directory(directory))
        self.analyzer = self.shared.analyzer(analyzer)
        self.filters, self.sorters, self.spellcheckers = {}, {}, {}
    @classmethod
    def load(cls, directory, analyzer=None):
        "Open `IndexSearcher`_ with a lucene RAMDirectory, loading index into memory."
        ref = closing()
        self = cls(lucene.RAMDirectory(ref.directory(directory)), analyzer)
        self.shared.add(self.directory)
        return self
    def __del__(self):
        if hash(self):
            self.close()
    def reopen(self, filters=False, sorters=False, spellcheckers=False):
        """Return current `IndexSearcher`_, only creating a new one if necessary.
        
        :param filters: refresh cached facet :attr:`filters`
        :param sorters: refresh cached :attr:`sorters` with associated parsers
        :param spellcheckers: refresh cached :attr:`spellcheckers`
        """
        if self.current:
            return self
        other = type(self)(self.indexReader.reopen(), self.analyzer)
        other.shared, other.owned = self.shared, closing([other.indexReader])
        other.filters.update((key, value if isinstance(value, lucene.Filter) else dict(value)) for key, value in self.filters.items())
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
        return Document(self.doc(id, lucene.MapFieldSelector(fields)))
    def parse(self, query, spellcheck=False, **kwargs):
        if isinstance(query, lucene.Query):
            return query
        if spellcheck:
            kwargs['parser'], kwargs['searcher'] = SpellParser, self
        return Analyzer.__dict__['parse'](self.analyzer, query, **kwargs)
    def highlighter(self, query, field, **kwargs):
        "Return `Highlighter`_ or if applicable `FastVectorHighlighter`_ specific to searcher and query."
        query = self.parse(query, field=field)
        vector = hasattr(lucene, 'FastVectorHighlighter') and field in self.names('termvector_with_position_offset')
        return (FastVectorHighlighter if vector else Highlighter)(self, query, field, **kwargs)
    def count(self, *query, **options):
        """Return number of hits for given query or term.
        
        :param query: :meth:`search` compatible query, or optimally a name and value
        :param options: additional :meth:`search` options
        """
        if len(query) > 1:
            return self.docFreq(lucene.Term(*query))
        query = self.parse(*query, **options) if query else lucene.MatchAllDocsQuery()
        if not hasattr(lucene, 'TotalHitCountCollector'):
            return lucene.IndexSearcher.search(self, query, options.get('filter'), 1, lucene.Sort.INDEXORDER).totalHits
        collector = lucene.TotalHitCountCollector()
        lucene.IndexSearcher.search(self, query, options.get('filter'), collector)
        return collector.totalHits
    def search(self, query=None, filter=None, count=None, sort=None, reverse=False, scores=False, maxscore=False, timeout=None, **parser):
        """Run query and return `Hits`_.
        
        :param query: query string or lucene Query
        :param filter: lucene Filter
        :param count: maximum number of hits to retrieve
        :param sort: if count is given, lucene Sort parameters, else a callable key
        :param reverse: reverse flag used with sort
        :param scores: compute scores for candidate results when using lucene Sort
        :param maxscore: compute maximum score of all results when using lucene Sort
        :param timeout: stop search after elapsed number of seconds
        :param parser: :meth:`Analyzer.parse` options
        """
        query = lucene.MatchAllDocsQuery() if query is None else self.parse(query, **parser)
        weight = self.createNormalizedWeight(query) if hasattr(self, 'createNormalizedWeight') else query.weight(self)
        # use custom Collector if all results are necessary, otherwise use lucene's TopDocsCollectors
        if count is None:
            collector = Collector()
        else:
            count, inorder = min(count, self.maxDoc()), not weight.scoresDocsOutOfOrder()
            if sort is None:
                collector = lucene.TopScoreDocCollector.create(count, inorder)
            else:
                if isinstance(sort, basestring):
                    sort = self.sorter(sort, reverse=reverse)
                if not isinstance(sort, lucene.Sort):
                    sort = lucene.Sort(sort)
                collector = lucene.TopFieldCollector.create(sort, count, False, scores, maxscore, inorder)
        results = collector if timeout is None else lucene.TimeLimitingCollector(collector, long(timeout * 1000))
        try:
            lucene.IndexSearcher.search(self, weight, filter, results)
        except lucene.JavaError as timeout:
            if not lucene.TimeLimitingCollector.TimeExceededException.instance_(timeout.getJavaException()):
                raise
        if isinstance(collector, Collector):
            ids, scores = collector.sorted(key=sort, reverse=reverse)
            collector.finalize()
            stats = len(ids), max(scores or [float('nan')])
        else:
            topdocs = collector.topDocs()
            scoredocs = list(topdocs.scoreDocs)
            ids, scores = (list(map(operator.attrgetter(name), scoredocs)) for name in ('doc', 'score'))
            stats = topdocs.totalHits, topdocs.maxScore
        stats *= not isinstance(timeout, lucene.JavaError)
        return Hits(self, ids, scores, *stats)
    def facets(self, query, *keys):
        """Return mapping of document counts for the intersection with each facet.
        
        :param ids: query string, lucene Query, or lucene Filter
        :param keys: field names, term tuples, or any keys to previously cached filters
        """
        counts = collections.defaultdict(dict)
        if isinstance(query, basestring):
            query = self.parse(query)
        if isinstance(query, lucene.Query):
            query = lucene.QueryWrapperFilter(query)
        if not isinstance(query, lucene.CachingWrapperFilter):
            query = lucene.CachingWrapperFilter(query)
        for key in keys:
            filters = self.filters.get(key)
            if isinstance(filters, lucene.Filter):
                counts[key] = self.overlap(query, filters)
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
                    counts[name][value] = self.overlap(query, filters[value])
        return dict(counts)
    def sorter(self, field, type='string', parser=None, reverse=False):
        "Return `SortField`_ with cached attributes if available."
        sorter = self.sorters.get(field, SortField(field, type, parser, reverse))
        return sorter if sorter.reverse == reverse else SortField(sorter.field, sorter.typename, sorter.parser, reverse)
    def comparator(self, field, type='string', parser=None):
        "Return :meth:`IndexReader.comparator` using a cached `SortField`_ if available."
        sorter = self.sorter(field, type, parser)
        if not hasattr(sorter, 'cache'):
            sorter.cache = sorter.comparator(self.indexReader)
        return sorter.cache
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
        searcher = lucene.MemoryIndex()
        for name, value in document.items():
            if isinstance(value, basestring):
                value = value, self.analyzer
            elif isinstance(value, lucene.TokenStream):
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
        if not lucene.MultiReader.instance_(reader):
            reader = lucene.MultiReader(list(map(shared.reader, reader)), True)
            self.owned = closing([reader])
        IndexSearcher.__init__(self, reader, analyzer)
        self.shared.update(shared)
        shared.clear()
    @property
    def version(self):
        return sum(map(operator.attrgetter('version'), self.sequentialSubReaders))
    @property
    def timestamp(self):
        return max(IndexReader(reader).timestamp for reader in self.sequentialSubReaders)
    def overlap(self, *filters):
        return sum(IndexReader(reader).overlap(*filters) for reader in self.sequentialSubReaders)

class IndexWriter(lucene.IndexWriter):
    """Inherited lucene IndexWriter.
    Supports setting fields parameters explicitly, so documents can be represented as dictionaries.
    
    :param directory: directory path or lucene Directory, default RAMDirectory
    :param mode: file mode (rwa), except updating (+) is implied
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    :param version: lucene Version argument passed to IndexWriterConfig or StandardAnalyzer, default is latest
    """
    __len__ = lucene.IndexWriter.numDocs
    __del__ = IndexSearcher.__dict__['__del__']
    parse = IndexSearcher.__dict__['parse']
    def __init__(self, directory=None, mode='a', analyzer=None, version=None):
        self.shared = closing()
        if version is None:
            version = lucene.Version.values()[-1]
        analyzer = self.shared.analyzer(analyzer, version)
        directory = self.shared.directory(directory)
        if hasattr(lucene, 'IndexWriterConfig'):
            config = lucene.IndexWriterConfig(version, analyzer)
            config.openMode = lucene.IndexWriterConfig.OpenMode.values()['wra'.index(mode)]
            lucene.IndexWriter.__init__(self, directory, config)
        else:
            args = [] if mode == 'a' else [bool('rw'.index(mode))]
            args.append(lucene.IndexWriter.MaxFieldLength.UNLIMITED)
            lucene.IndexWriter.__init__(self, directory, analyzer, *args)
        self.fields = {}
    def set(self, name, cls=Field, **params):
        """Assign parameters to field name.
        
        :param name: registered name of field
        :param cls: optional `Field`_ constructor
        :param params: store,index,termvector options compatible with `Field`_
        """
        self.fields[name] = cls(name, **params)
    def document(self, document=(), boost=1.0, **terms):
        "Return lucene Document from mapping of field names to one or multiple values."
        doc = lucene.Document()
        doc.boost = boost
        for name, values in dict(document, **terms).items():
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
        self.updateDocument(lucene.Term(name, *[value] if value else doc.getValues(name)), doc)
    def delete(self, *query, **options):
        """Remove documents which match given query or term.
        
        :param query: :meth:`IndexSearcher.search` compatible query, or optimally a name and value
        :param options: additional :meth:`Analyzer.parse` options
        """
        parse = self.parse if len(query) == 1 else lucene.Term
        self.deleteDocuments(parse(*query, **options))
    def __iadd__(self, directory):
        "Add directory (or reader, searcher, writer) to index."
        ref = closing()
        directory = ref.directory(directory)
        self.addIndexesNoOptimize([directory if isinstance(directory, lucene.Directory) else directory.directory])
        return self

class Indexer(IndexWriter):
    """An all-purpose interface to an index.
    Creates an `IndexWriter`_ with a delegated `IndexSearcher`_.
    
    :param nrt: optionally use a near real-time searcher
    """
    def __init__(self, directory=None, mode='a', analyzer=None, version=None, nrt=False):
        IndexWriter.__init__(self, directory, mode, analyzer, version)
        IndexWriter.commit(self)
        self.nrt = nrt
        if nrt:
            try:
                reader = lucene.IndexReader.open(self, True)
            except lucene.InvalidArgsError:
                reader = self.reader
            self.indexSearcher = IndexSearcher(reader, self.analyzer)
            self.indexSearcher.owned = closing([reader])
        else:
            self.indexSearcher = IndexSearcher(self.directory, self.analyzer)
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
    def commit(self, expunge=False, optimize=False, **caches):
        """Commit writes and :meth:`refresh` searcher.
        
        :param expunge: expunge deletes
        :param optimize: optimize index, optionally supply number of segments
        """
        IndexWriter.commit(self)
        if expunge:
            self.expungeDeletes()
            IndexWriter.commit(self)
        if optimize:
            self.optimize(optimize)
            IndexWriter.commit(self)
        self.refresh(**caches)
