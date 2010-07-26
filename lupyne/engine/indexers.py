"""
Wrappers for lucene Index{Read,Search,Writ}ers.

The final `Indexer`_ classes exposes a high-level Searcher and Writer.
"""

import itertools, operator
import contextlib
import abc, collections
import lucene
from .queries import Query, HitCollector, Filter, Highlighter, SpellChecker, SpellParser
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

class TokenFilter(lucene.PythonTokenFilter):
    """Create an iterable lucene TokenFilter from a TokenStream.
    In lucene version 2, call on any iterable of tokens.
    In lucene version 3, subclass and override :meth:`incrementToken`;
    attributes are cached as properties to create a Token interface.
    """
    def __init__(self, input):
        if issubclass(lucene.TokenFilter, collections.Iterable):
            lucene.PythonTokenFilter.__init__(self, lucene.EmptyTokenStream())
            self.next = iter(input).next
        else:
            lucene.PythonTokenFilter.__init__(self, input)
            self.input = input
    def __iter__(self):
        return self
    if not hasattr(lucene.TokenFilter, 'next'):
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
    :param filters: lucene TokenFilters or python generators.
    """
    def __init__(self, tokenizer, *filters):
        lucene.PythonAnalyzer.__init__(self)
        self.tokenizer, self.filters = tokenizer, filters
    def tokenStream(self, field, reader):
        tokens = self.tokenizer.tokenStream(field, reader) if isinstance(self.tokenizer, lucene.Analyzer) else self.tokenizer(reader)
        for filter in self.filters:
            tokens = filter(tokens)
        return tokens if isinstance(tokens, lucene.TokenStream) else TokenFilter(tokens)
    def tokens(self, text, field=None):
        "Return lucene TokenStream from text."
        return self.tokenStream(field, lucene.StringReader(text))
    def parse(self, query, field='', op='', version=lucene.VERSION[:3], parser=None, **attrs):
        """Return parsed lucene Query.
        
        :param query: query string
        :param field: default query field name, sequence of names, or boost mapping
        :param op: default query operator ('or', 'and')
        :param version: lucene Version string, leave blank for deprecated constructor
        :param parser: custom PythonQueryParser class
        :param attrs: additional attributes to set on the parser
        """
        # parsers aren't thread-safe (nor slow), so create one each time
        version = getattr(lucene.Version, 'LUCENE_' + version.replace('.', ''))
        args = [] if lucene.VERSION.startswith('2.9') and parser is not None else [version]
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
            return lucene.MultiFieldQueryParser.parse(parser, query) # bug in method binding
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
            termenum = self.indexReader.terms(term)
        with contextlib.closing(termenum):
            term = termenum.term()
            while term and term.field() == name:
                text = term.text()
                if stop is not None and text >= stop:
                    break
                yield (text, termenum.docFreq()) if counts else text
                term = termenum.next() and termenum.term()
    def docs(self, name, value, counts=False):
        "Generate doc ids which contain given term, optionally with frequency counts."
        with contextlib.closing(self.termDocs(lucene.Term(name, value))) as termdocs:
            while termdocs.next():
                doc = termdocs.doc()
                yield (doc, termdocs.freq()) if counts else doc
    def positions(self, name, value, payloads=False):
        "Generate doc ids and positions which contain given term, optionally only with payloads."
        with contextlib.closing(self.termPositions(lucene.Term(name, value))) as termpositions:
            while termpositions.next():
                doc = termpositions.doc()
                positions = (termpositions.nextPosition() for n in xrange(termpositions.freq()))
                if payloads:
                    items, array = [], lucene.JArray_byte('')
                    for position in positions:
                        if termpositions.payloadAvailable:
                            data = termpositions.getPayload(array, 0)
                            items.append((position, data.string_ if hasattr(data, 'string_') else ''.join(data)))
                    yield doc, items
                else:
                    yield doc, list(positions)
    def comparator(self, name, type='string', parser=None):
        """Return cache of field values suitable for sorting.
        Parsing values into an array is memory optimized.
        Map values into a list for speed optimization.
        
        :param name: field name
        :param type: type of field values compatible with lucene FieldCache
        :param parser: callable applied to field values
        """
        type = getattr(type, '__name__', type).capitalize()
        method = getattr(lucene.FieldCache.DEFAULT, 'get{0}s'.format(type))
        if parser is None:
            return method(self.indexReader, name)
        bases = getattr(lucene, 'Python{0}Parser'.format(type)),
        parser = object.__class__('', bases, {'parse' + type: staticmethod(parser)})
        return method(self.indexReader, name, parser())
    def spans(self, query, positions=False, payloads=False):
        """Generate docs with occurrence counts for a span query.
        
        :param query: lucene SpanQuery
        :param positions: optionally include slice positions instead of counts
        :param payloads: optionally only include slice positions with payloads
        """
        spans = query.getSpans(self.indexReader)
        spans = itertools.takewhile(lucene.Spans.next, itertools.repeat(spans))
        for doc, spans in itertools.groupby(spans, key=lucene.Spans.doc):
            if payloads:
                items = []
                for span in spans:
                    if span.payloadAvailable:
                        payloads = map(lucene.JArray_byte.cast_, span.payload)
                        payloads = [data.string_ if hasattr(data, 'string_') else ''.join(data) for data in payloads]
                        items.append((span.start(), span.end(), payloads))
                yield doc, items
            elif positions:
                yield doc, [(span.start(), span.end()) for span in spans]
            else:
                yield doc, sum(1 for span in spans)
    def termvector(self, id, field, counts=False):
        "Generate terms for given doc id and field, optionally with frequency counts."
        tfv = self.getTermFreqVector(id, field)
        return itertools.izip(tfv.terms, tfv.termFrequencies) if counts else iter(tfv.terms)
    def positionvector(self, id, field, offsets=False):
        "Generate terms and positions for given doc id and field, optionally with character offsets."
        tpv = lucene.TermPositionVector.cast_(self.getTermFreqVector(id, field))
        for index, term in enumerate(tpv.terms):
            if offsets:
                yield term, map(operator.attrgetter('startOffset', 'endOffset'), tpv.getOffsets(index))
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

class Searcher(object):
    "Mixin interface common among searchers."
    def __init__(self, arg, analyzer=None):
        super(Searcher, self).__init__(arg)
        if analyzer is None:
            analyzer = lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
            self.shared.add(analyzer)
        self.analyzer = analyzer
        self.filters = {}
        self.spellcheckers = {}
    def __del__(self):
        if hash(self):
            self.close()
    def open(self, *directories):
        self.shared = closing()
        for directory in directories:
            if isinstance(directory, basestring):
                directory = lucene.FSDirectory.open(lucene.File(directory))
                self.shared.add(directory)
            yield directory
    def reopen(self, filters=False, spellcheckers=False):
        """Return current Searcher, only creating a new one if necessary.
        
        :param filters: refresh cached facet :attr:`filters`
        :param spellcheckers: refresh cached :attr:`spellcheckers`
        """
        reader = self.indexReader.reopen()
        if reader == self.indexReader:
            return self
        other = type(self)(reader, self.analyzer)
        other.shared, other.owned = self.shared, closing([reader])
        if filters:
            other.filters.update((key, value if isinstance(value, lucene.Filter) else dict(value)) for key, value in self.filters.items())
            other.facets([], *other.filters)
        if spellcheckers:
            for field in self.spellcheckers:
                other.suggest(field, '')
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
        return Analyzer.parse.im_func(self.analyzer, query, **kwargs)
    def highlighter(self, query, span=True, formatter=None, encoder=None, field=None):
        "Return `Highlighter`_ specific to the searcher's analyzer and index."
        query = self.parse(query, field=field or '')
        return Highlighter(query, self.analyzer, span, formatter, encoder, field, (field and self.indexReader))
    def highlight(self, query, doc, count=1, span=True, formatter=None, encoder=None, field=None, **attrs):
        """Return highlighted text fragments which match the query, using internal :meth:`highlighter`.
        
        :param query: query string or lucene Query
        :param doc: text string or doc id to be highlighted
        :param count: maximum number of fragments
        :param attrs: additional attributes to set on the highlighter
        """
        highlighter = self.highlighter(query, span, formatter, encoder, field)
        for name, value in attrs.items():
            setattr(highlighter, name, value)
        if not isinstance(doc, basestring):
            doc = self[doc][field]
        return highlighter.fragments(doc, count)
    def count(self, *query, **options):
        """Return number of hits for given query or term.
        
        :param query: :meth:`search` compatible query, or optimally a name and value
        :param options: additional :meth:`search` options
        """
        if len(query) <= 1:
            return self.search(*query, count=1, sort=lucene.Sort.INDEXORDER, **options).count
        return self.docFreq(lucene.Term(*query))
    def search(self, query=None, filter=None, count=None, sort=None, reverse=False, scores=False, maxscore=False, timeout=None, **parser):
        """Run query and return `Hits`_.
        
        :param query: query string or lucene Query
        :param filter: doc ids or lucene Filter
        :param count: maximum number of hits to retrieve
        :param sort: if count is given, lucene Sort parameters, else a callable key
        :param reverse: reverse flag used with sort
        :param scores: compute scores for candidate results when using lucene Sort
        :param maxscore: compute maximum score of all results when using lucene Sort
        :param timeout: stop search after elapsed number of seconds
        :param parser: :meth:`Analyzer.parse` options
        """
        query = lucene.MatchAllDocsQuery() if query is None else self.parse(query, **parser)
        if not isinstance(filter, (lucene.Filter, type(None))):
            filter = Filter(filter)
        weight = query.weight(self)
        # use custom HitCollector if all results are necessary, otherwise use lucene's TopDocsCollectors
        if count is None:
            collector = HitCollector()
        else:
            count, inorder = min(count, self.maxDoc()), not weight.scoresDocsOutOfOrder()
            if sort is None:
                collector = lucene.TopScoreDocCollector.create(count, inorder)
            else:
                if isinstance(sort, basestring):
                    sort = lucene.SortField(sort, lucene.SortField.STRING, reverse)
                if not isinstance(sort, lucene.Sort):
                    sort = lucene.Sort(sort)
                collector = lucene.TopFieldCollector.create(sort, count, False, scores, maxscore, inorder)
        TimeLimitingCollector = lucene.TimeLimitingCollector if isinstance(collector, lucene.Collector) else lucene.TimeLimitedCollector
        results = collector if timeout is None else TimeLimitingCollector(collector, long(timeout * 1000))
        try:
            super(Searcher, self).search(weight, filter, results)
        except lucene.JavaError as timeout:
            if not TimeLimitingCollector.TimeExceededException.instance_(timeout.getJavaException()):
                raise
        if isinstance(collector, HitCollector):
            ids, scores = collector.sorted(key=sort, reverse=reverse)
            collector.finalize()
            stats = len(ids), max(scores or [float('nan')])
        else:
            topdocs = collector.topDocs()
            scoredocs = list(topdocs.scoreDocs)
            ids, scores = (map(operator.attrgetter(name), scoredocs) for name in ('doc', 'score'))
            stats = topdocs.totalHits, topdocs.maxScore
        stats *= not isinstance(timeout, lucene.JavaError)
        return Hits(self, ids, scores, *stats)
    def facets(self, ids, *keys):
        """Return mapping of document counts for the intersection with each facet.
        
        :param ids: document ids or lucene Filter
        :param keys: field names, term tuples, or any keys to previously cached filters
        """
        counts = collections.defaultdict(dict)
        if not isinstance(ids, lucene.Filter):
            ids = Filter(ids)
        for key in keys:
            filters = self.filters.get(key)
            if isinstance(filters, lucene.Filter):
                counts[key] = Filter.overlap(ids, filters, self.indexReader)
            elif isinstance(key, basestring):
                values = self.terms(key) if filters is None else filters
                counts.update(self.facets(ids, *((key, value) for value in values)))
            else:
                name, value = key
                filters = self.filters.setdefault(name, {})
                if value not in filters:
                    filters[value] = Query.term(name, value).filter()
                counts[name][value] = Filter.overlap(ids, filters[value], self.indexReader)
        return dict(counts)
    def suggest(self, field, prefix, count=None):
        "Return ordered suggested words for prefix."
        if field not in self.spellcheckers:
            self.spellcheckers[field] = SpellChecker(self.terms(field, counts=True))
        return self.spellcheckers[field].suggest(prefix, count)
    def correct(self, field, text, distance=2, minSimilarity=0.5):
        """Generate potential words ordered by increasing edit distance and decreasing frequency.
        For optimal performance only iterate the required slice size of corrections.
        
        :param distance: the maximum edit distance to consider for enumeration
        :param minSimilarity: threshold for additional fuzzy terms after edits have been exhausted
        """
        if field not in self.spellcheckers:
            self.spellcheckers[field] = SpellChecker(self.terms(field, counts=True))
        corrections = set()
        for words in itertools.islice(self.spellcheckers[field].correct(text), distance + 1):
            for word in words:
                yield word
            corrections.update(words)
        if minSimilarity:
            words = set(self.terms(field, text, minSimilarity=minSimilarity)) - corrections
            for word in sorted(words, key=self.spellcheckers[field].__getitem__, reverse=True):
                yield word

class IndexSearcher(Searcher, lucene.IndexSearcher, IndexReader):
    """Inherited lucene IndexSearcher, with a mixed-in IndexReader.
    
    :param directory: directory path, lucene Directory, or lucene IndexReader
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    """
    def __init__(self, directory, analyzer=None):
        Searcher.__init__(self, *self.open(directory), analyzer=analyzer)

class MultiSearcher(Searcher, lucene.MultiSearcher, IndexReader):
    """Inherited lucene MultiSearcher.
    All sub searchers will be closed when the MultiSearcher is closed.
    
    :param searchers: directory paths, Directories, IndexReaders, Searchers, or a MultiReader
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    """
    def __init__(self, searchers, analyzer=None):
        if lucene.MultiReader.instance_(searchers):
            self.indexReader = searchers
            searchers = map(lucene.IndexSearcher, searchers.sequentialSubReaders)
        else:
            searchers = [searcher if isinstance(searcher, lucene.Searcher) else lucene.IndexSearcher(searcher) for searcher in self.open(*searchers)]
            self.indexReader = lucene.MultiReader(map(operator.attrgetter('indexReader'), searchers), False)
            self.owned = closing([self.indexReader])
        Searcher.__init__(self, searchers, analyzer)

class ParallelMultiSearcher(MultiSearcher, lucene.ParallelMultiSearcher):
    "Inherited lucene ParallelMultiSearcher."

class IndexWriter(lucene.IndexWriter):
    """Inherited lucene IndexWriter.
    Supports setting fields parameters explicitly, so documents can be represented as dictionaries.
    
    :param directory: directory path or lucene Directory, default RAMDirectory
    :param mode: file mode (rwa), except updating (+) is implied
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    """
    __len__ = lucene.IndexWriter.numDocs
    __del__ = IndexSearcher.__del__.im_func
    parse = Searcher.parse.im_func
    def __init__(self, directory=None, mode='a', analyzer=None):
        self.shared = closing()
        if analyzer is None:
            analyzer = lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
            self.shared.add(analyzer)
        if not isinstance(directory, lucene.Directory):
            directory = lucene.RAMDirectory() if directory is None else lucene.FSDirectory.open(lucene.File(directory))
            self.shared.add(directory)
        mfl = lucene.IndexWriter.MaxFieldLength.LIMITED
        if mode == 'a':
            lucene.IndexWriter.__init__(self, directory, analyzer, mfl)
        else:
            lucene.IndexWriter.__init__(self, directory, analyzer, bool('rw'.index(mode)), mfl)
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
    def add(self, document=(), boost=1.0, **terms):
        """Add document to index.
        Document is comprised of name: value pairs, where the values may be one or multiple strings.
        
        :param document: optional document terms as a dict or items
        :param terms: additional terms to document
        """
        terms.update(document)
        doc = lucene.Document()
        doc.boost = boost
        for name, values in terms.items():
            if isinstance(values, Atomic):
                values = [values] 
            for field in self.fields[name].items(*values):
                doc.add(field)
        self.addDocument(doc)
    def delete(self, *query, **options):
        """Remove documents which match given query or term.
        
        :param query: :meth:`Searcher.search` compatible query, or optimally a name and value
        :param options: additional :meth:`Analyzer.parse` options
        """
        if len(query) == 1:
            self.deleteDocuments(self.parse(*query, **options))
        else:
            self.deleteDocuments(lucene.Term(*query))
    def __iadd__(self, directory):
        "Add directory (or reader, searcher, writer) to index."
        if isinstance(directory, basestring):
            with contextlib.closing(lucene.FSDirectory.open(lucene.File(directory))) as directory:
                self.addIndexesNoOptimize([directory])
        else:
            if not isinstance(directory, lucene.Directory):
                directory = directory.directory
            self.addIndexesNoOptimize([directory])
        return self

class Indexer(IndexWriter):
    """An all-purpose interface to an index.
    Creates an `IndexWriter`_ with a delegated `IndexSearcher`_.
    """
    def __init__(self, *args, **kwargs):
        IndexWriter.__init__(self, *args, **kwargs)
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
    def commit(self, **caches):
        """Commit writes and refresh searcher.  Not thread-safe.
        
        :param caches: :meth:`Searcher.reopen` caches to refresh
        """
        IndexWriter.commit(self)
        self.indexSearcher = self.indexSearcher.reopen(**caches)
