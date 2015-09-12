"""
Wrappers for lucene Index{Read,Search,Writ}ers.

The final `Indexer`_ classes exposes a high-level Searcher and Writer.
"""

from future_builtins import filter, map, zip
import os
import itertools
import operator
import contextlib
import collections
import lucene
from java.io import File, StringReader
from java.lang import Float
from java.util import Arrays, HashMap, HashSet
from org.apache.lucene import analysis, document, index, queries, queryparser, search, store, util
from org.apache.pylucene.analysis import PythonAnalyzer, PythonTokenFilter
from org.apache.pylucene.queryparser.classic import PythonQueryParser
from .queries import suppress, Query, BooleanFilter, TermsFilter, SortField, Highlighter, FastVectorHighlighter, SpellChecker, SpellParser
from .documents import Field, Document, Hits, GroupingSearch
from .spatial import DistanceComparator
from ..utils import Atomic, method

for cls in (analysis.TokenStream, lucene.JArray_byte):
    Atomic.register(cls)


class closing(set):
    "Manage lifespan of registered objects, similar to contextlib.closing."
    def __del__(self):
        for obj in self:
            obj.close()

    def analyzer(self, analyzer, version=None):
        if analyzer is None:
            analyzer = analysis.standard.StandardAnalyzer(version or util.Version.LATEST)
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
        if isinstance(reader, index.IndexReader):
            reader.incRef()
        elif isinstance(reader, index.IndexWriter):
            reader = index.IndexReader.open(reader, True)
        elif isinstance(reader, Atomic):
            reader = index.IndexReader.open(self.directory(reader))
        else:
            reader = index.MultiReader(list(map(self.reader, reader)))
        return reader

    @classmethod
    @contextlib.contextmanager
    def store(cls, directory):
        self = cls()
        yield self.directory(directory)


def copy(commit, dest):
    """Copy the index commit to the destination directory.
    Optimized to use hard links if the destination is a file system path.
    """
    if isinstance(dest, store.Directory):
        for filename in commit.fileNames:
            commit.directory.copy(dest, filename, filename, store.IOContext.DEFAULT)
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
    def __iter__(self):
        self.reset()
        return self

    def next(self):
        if self.incrementToken():
            return self
        raise StopIteration

    def __getattr__(self, name):
        cls = getattr(analysis.tokenattributes, name + 'Attribute').class_
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
        return payload and payload.utf8ToString()
    @payload.setter
    def payload(self, data):
        self.Payload.payload = util.BytesRef(data)

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
        return self.CharTerm.toString()
    @term.setter
    def term(self, text):
        self.CharTerm.setEmpty()
        self.CharTerm.append(text)

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
        return source, tokens

    def createComponents(self, field, reader):
        return analysis.Analyzer.TokenStreamComponents(*self.components(field, reader))

    def tokens(self, text, field=None):
        "Return lucene TokenStream from text."
        return self.components(field, StringReader(text))[1]

    @method
    def parse(self, query, field='', op='', version=None, parser=None, **attrs):
        """Return parsed lucene Query.
        
        :param query: query string
        :param field: default query field name, sequence of names, or boost mapping
        :param op: default query operator ('or', 'and')
        :param version: lucene Version
        :param parser: custom PythonQueryParser class
        :param attrs: additional attributes to set on the parser
        """
        # parsers aren't thread-safe (nor slow), so create one each time
        cls = queryparser.classic.QueryParser if isinstance(field, basestring) else queryparser.classic.MultiFieldQueryParser
        args = field, self
        if isinstance(field, collections.Mapping):
            boosts = HashMap()
            for key in field:
                boosts.put(key, Float(field[key]))
            args = list(field), self, boosts
        parser = (parser or cls)(version or util.Version.LATEST, *args)
        if op:
            parser.defaultOperator = getattr(queryparser.classic.QueryParser.Operator, op.upper())
        for name, value in attrs.items():
            setattr(parser, name, value)
        if isinstance(parser, queryparser.classic.MultiFieldQueryParser):
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
        return getattr(index.DirectoryReader.cast_(self.indexReader), name)

    def __len__(self):
        return self.numDocs()

    def __contains__(self, id):
        bits = self.bits
        return (0 <= id < self.maxDoc()) and (not bits or bits.get(id))

    def __iter__(self):
        ids = xrange(self.maxDoc())
        bits = self.bits
        return filter(bits.get, ids) if bits else iter(ids)

    @property
    def bits(self):
        return index.MultiFields.getLiveDocs(self.indexReader)

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
        directory = store.FSDirectory.cast_(self.directory).directory
        return File(directory, self.indexCommit.segmentsFileName).lastModified() * 0.001

    @property
    def readers(self):
        "segment readers"
        return (index.SegmentReader.cast_(context.reader()) for context in self.leaves())

    @property
    def segments(self):
        "segment filenames with document counts"
        return {reader.segmentName: reader.numDocs() for reader in self.readers}

    def copy(self, dest, query=None, exclude=None, merge=0):
        """Copy the index to the destination directory.
        Optimized to use hard links if the destination is a file system path.
        
        :param dest: destination directory path or lucene Directory
        :param query: optional lucene Query to select documents
        :param exclude: optional lucene Query to exclude documents
        :param merge: optionally merge into maximum number of segments
        """
        copy(self.indexCommit, dest)
        with IndexWriter(dest) as writer:
            if query:
                writer.delete(Query.alldocs() - query)
            if exclude:
                writer.delete(exclude)
            writer.commit()
            writer.forceMergeDeletes()
            if merge:
                writer.forceMerge(merge)
            return len(writer)

    def names(self, **attrs):
        """Return field names, given option description.
        
        .. versionchanged:: 1.2 lucene requires FieldInfo filter attributes instead of option
        """
        fieldinfos = index.MultiFields.getMergedFieldInfos(self.indexReader).iterator()
        return [fieldinfo.name for fieldinfo in fieldinfos if all(getattr(fieldinfo, name) == attrs[name] for name in attrs)]

    def terms(self, name, value='', stop=None, counts=False, distance=0):
        """Generate a slice of term values, optionally with frequency counts.
        Supports a range of terms, wildcard terms, or fuzzy terms.
        
        :param name: field name
        :param value: term prefix or lower bound for range terms
        :param stop: optional upper bound for range terms
        :param counts: include frequency counts
        :param distance: maximum edit distance for fuzzy terms
        """
        terms = index.MultiFields.getTerms(self.indexReader, name)
        termsenum = terms.iterator(None) if terms else index.TermsEnum.EMPTY
        if terms and distance:
            termsenum = search.FuzzyTermsEnum(terms, util.AttributeSource(), index.Term(name, value), float(distance), 0, False)
        elif stop is not None:
            termsenum = search.TermRangeTermsEnum(termsenum, util.BytesRef(value), util.BytesRef(stop), True, False)
        else:
            termsenum = search.PrefixTermsEnum(termsenum, util.BytesRef(value))
        terms = map(operator.methodcaller('utf8ToString'), termsenum)
        return ((term, termsenum.docFreq()) for term in terms) if counts else terms

    def numbers(self, name, step=0, type=int, counts=False):
        """Generate decoded numeric term values, optionally with frequency counts.
        
        :param name: field name
        :param step: precision step to select terms
        :param type: int or float
        :param counts: include frequency counts
        """
        convert = util.NumericUtils.sortableLongToDouble if issubclass(type, float) else int
        termsenum = index.MultiFields.getTerms(self.indexReader, name).iterator(None)
        termsenum = search.PrefixTermsEnum(termsenum, util.BytesRef(chr(ord(' ') + step)))
        values = map(convert, map(util.NumericUtils.prefixCodedToLong, termsenum))
        return ((value, termsenum.docFreq()) for value in values) if counts else values

    def docs(self, name, value, counts=False):
        "Generate doc ids which contain given term, optionally with frequency counts."
        docsenum = index.MultiFields.getTermDocsEnum(self.indexReader, self.bits, name, util.BytesRef(value))
        docs = iter(docsenum.nextDoc, index.DocsEnum.NO_MORE_DOCS) if docsenum else ()
        return ((doc, docsenum.freq()) for doc in docs) if counts else iter(docs)

    def positions(self, name, value, payloads=False, offsets=False):
        "Generate doc ids and positions which contain given term, optionally with offsets, or only ones with payloads."
        docsenum = index.MultiFields.getTermPositionsEnum(self.indexReader, self.bits, name, util.BytesRef(value))
        for doc in (iter(docsenum.nextDoc, index.DocsEnum.NO_MORE_DOCS) if docsenum else ()):
            positions = (docsenum.nextPosition() for n in xrange(docsenum.freq()))
            if payloads:
                positions = ((position, docsenum.payload.utf8ToString()) for position in positions if docsenum.payload)
            elif offsets:
                positions = ((docsenum.startOffset(), docsenum.endOffset()) for position in positions)
            yield doc, list(positions)

    def spans(self, query, positions=False, payloads=False):
        """Generate docs with occurrence counts for a span query.
        
        :param query: lucene SpanQuery
        :param positions: optionally include slice positions instead of counts
        :param payloads: optionally only include slice positions with payloads
        """
        offset = 0
        for reader in self.readers:
            spans = itertools.repeat(query.getSpans(reader.context, reader.liveDocs, HashMap()))
            for doc, spans in itertools.groupby(itertools.takewhile(search.spans.Spans.next, spans), key=search.spans.Spans.doc):
                if payloads:
                    values = [(span.start(), span.end(), [lucene.JArray_byte.cast_(data).string_ for data in span.payload])
                              for span in spans if span.payloadAvailable]
                elif positions:
                    values = [(span.start(), span.end()) for span in spans]
                else:
                    values = sum(1 for span in spans)
                yield (doc + offset), values
            offset += reader.maxDoc()

    def vector(self, id, field):
        terms = self.getTermVector(id, field)
        termsenum = terms.iterator(None) if terms else index.TermsEnum.EMPTY
        return termsenum, map(operator.methodcaller('utf8ToString'), util.BytesRefIterator.cast_(termsenum))

    def termvector(self, id, field, counts=False):
        "Generate terms for given doc id and field, optionally with frequency counts."
        termsenum, terms = self.vector(id, field)
        return ((term, int(termsenum.totalTermFreq())) for term in terms) if counts else terms

    def positionvector(self, id, field, offsets=False):
        "Generate terms and positions for given doc id and field, optionally with character offsets."
        termsenum, terms = self.vector(id, field)
        for term in terms:
            docsenum = termsenum.docsAndPositions(None, None)
            assert 0 <= docsenum.nextDoc() < docsenum.NO_MORE_DOCS
            positions = (docsenum.nextPosition() for n in xrange(docsenum.freq()))
            if offsets:
                positions = ((docsenum.startOffset(), docsenum.endOffset()) for position in positions)
            yield term, list(positions)

    def morelikethis(self, doc, *fields, **attrs):
        """Return MoreLikeThis query for document.
        
        :param doc: document id or text
        :param fields: document fields to use, optional for termvectors
        :param attrs: additional attributes to set on the morelikethis object
        """
        mlt = queries.mlt.MoreLikeThis(self.indexReader)
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
        self.termsfilters = set()

    @classmethod
    def load(cls, directory, analyzer=None):
        "Open `IndexSearcher`_ with a lucene RAMDirectory, loading index into memory."
        with closing.store(directory) as directory:
            directory = store.RAMDirectory(directory, store.IOContext.DEFAULT)
        self = cls(directory, analyzer)
        self.shared.add(self.directory)
        return self

    def __del__(self):
        if hash(self):
            self.decRef()

    def openIfChanged(self):
        return index.DirectoryReader.openIfChanged(index.DirectoryReader.cast_(self.indexReader))

    def reopen(self, filters=False, sorters=False, spellcheckers=False):
        """Return current `IndexSearcher`_, only creating a new one if necessary.
        Any registered :attr:`termsfilters` are also refreshed.
        
        :param filters: refresh cached facet :attr:`filters`
        :param sorters: refresh cached :attr:`sorters` with associated parsers
        :param spellcheckers: refresh cached :attr:`spellcheckers`
        """
        reader = self.openIfChanged()
        if reader is None:
            return self
        other = type(self)(reader, self.analyzer)
        other.decRef()
        other.shared = self.shared
        other.filters.update((key, value if isinstance(value, search.Filter) else dict(value)) for key, value in self.filters.items())
        other.termsfilters.update(self.termsfilters)
        for termsfilter in self.termsfilters:
            termsfilter.refresh(other)
        if filters:
            other.facets(Query.any(), *other.filters)
        other.sorters = {name: SortField(sorter.field, sorter.typename, sorter.parser) for name, sorter in self.sorters.items()}
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
        return Document(self.document(id, HashSet(Arrays.asList(fields))))

    def parse(self, query, spellcheck=False, **kwargs):
        if isinstance(query, search.Query):
            return query
        if spellcheck:
            kwargs['parser'], kwargs['searcher'] = SpellParser, self
        return Analyzer.parse(self.analyzer, query, **kwargs)

    def highlighter(self, query, field, **kwargs):
        "Return `Highlighter`_ or if applicable `FastVectorHighlighter`_ specific to searcher and query."
        query = self.parse(query, field=field)
        fieldinfo = index.MultiFields.getMergedFieldInfos(self.indexReader).fieldInfo(field)
        vector = fieldinfo and fieldinfo.hasVectors()
        return (FastVectorHighlighter if vector else Highlighter)(self, query, field, **kwargs)

    def count(self, *query, **options):
        """Return number of hits for given query or term.
        
        :param query: :meth:`search` compatible query, or optimally a name and value
        :param options: additional :meth:`search` options
        """
        if len(query) > 1:
            return self.docFreq(index.Term(*query))
        query = self.parse(*query, **options) if query else Query.alldocs()
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
        query = Query.alldocs() if query is None else self.parse(query, **parser)
        cache = collector = self.collector(query, count, sort, reverse, scores, maxscore)
        counter = search.TimeLimitingCollector.getGlobalCounter()
        results = collector if timeout is None else search.TimeLimitingCollector(collector, counter, long(timeout * 1000))
        with suppress(search.TimeLimitingCollector.TimeExceededException):
            search.IndexSearcher.search(self, query, filter, results)
            timeout = None
        if isinstance(cache, search.CachingCollector):
            collector = search.TotalHitCountCollector()
            cache.replay(collector)
            collector = self.collector(query, collector.totalHits or 1, sort, reverse, scores, maxscore)
            cache.replay(collector)
        topdocs = collector.topDocs()
        stats = (topdocs.totalHits, topdocs.maxScore) * (timeout is None)
        return Hits(self, topdocs.scoreDocs, *stats)

    def facets(self, query, *keys):
        """Return mapping of document counts for the intersection with each facet.
        
        .. versionchanged:: 1.6 filters are no longer implicitly cached, a `GroupingSearch`_ is used instead
        
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
            if isinstance(filters, search.Filter):
                counts[key] = self.count(filter=BooleanFilter.all(query, filters))
            elif isinstance(filters, collections.Mapping):
                for value in filters:
                    counts[key][value] = self.count(filter=BooleanFilter.all(query, filters[value]))
            elif isinstance(key, basestring):
                counts[key] = self.groupby(key, Query.alldocs(), filter=query).facets
            else:
                name, value = key
                counts[name][value] = self.count(filter=BooleanFilter.all(query, self.filters[name][value]))
        return dict(counts)

    def groupby(self, field, query, filter=None, count=None, start=0, **attrs):
        "Return `Hits`_ grouped by field using a `GroupingSearch`_."
        return GroupingSearch(field, **attrs).search(self, self.parse(query), filter, count, start)

    def sorter(self, field, type='string', parser=None, reverse=False):
        "Return `SortField`_ with cached attributes if available."
        sorter = self.sorters.get(field, SortField(field, type, parser, reverse))
        return sorter if sorter.reverse == reverse else SortField(sorter.field, sorter.typename, sorter.parser, reverse)

    def comparator(self, field, type='string', parser=None, multi=False):
        """Return cache of field values suitable for sorting, using a cached `SortField`_ if available.
        Parsing values into an array is memory optimized.
        Map values into a list for speed optimization.
        Comparators are not thread-safe.
        
        :param name: field name
        :param type: type object or name compatible with FieldCache
        :param parser: lucene FieldCache.Parser or callable applied to field values
        :param multi: retrieve multi-valued string terms as a tuple
        """
        return self.sorter(field, type, parser).comparator(self, multi)

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

    def correct(self, field, text, distance=2):
        """Generate potential words ordered by increasing edit distance and decreasing frequency.
        For optimal performance only iterate the required slice size of corrections.
        
        :param distance: the maximum edit distance to consider for enumeration
        """
        return itertools.chain.from_iterable(itertools.islice(self.spellchecker(field).correct(text), distance + 1))

    def match(self, document, *queries):
        "Generate scores for all queries against a given document mapping."
        searcher = index.memory.MemoryIndex()
        for name, value in document.items():
            if isinstance(value, basestring):
                value = value, self.analyzer
            elif isinstance(value, analysis.TokenStream):
                value = value,
            searcher.addField(name, *value)
        return (searcher.search(self.parse(query)) for query in queries)

    def termsfilter(self, field, values=()):
        """Return registered `TermsFilter`_, which will be refreshed whenever the searcher is reopened.
        
        .. versionadded:: 1.7
        .. note:: This interface is experimental and might change in incompatible ways in the next release.
        """
        termsfilter = TermsFilter(field, values)
        termsfilter.refresh(self)
        self.termsfilters.add(termsfilter)
        return termsfilter


class MultiSearcher(IndexSearcher):
    """IndexSearcher with underlying lucene MultiReader.
    
    :param reader: directory paths, Directories, IndexReaders, or a single MultiReader
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    """
    def __init__(self, reader, analyzer=None):
        IndexSearcher.__init__(self, reader, analyzer)
        self.indexReaders = [index.DirectoryReader.cast_(context.reader()) for context in self.context.children()]
        self.version = sum(reader.version for reader in self.indexReaders)

    def __getattr__(self, name):
        return getattr(index.MultiReader.cast_(self.indexReader), name)

    def openIfChanged(self):
        readers = list(map(index.DirectoryReader.openIfChanged, self.indexReaders))
        if any(readers):
            return index.MultiReader([new or old.incRef() or old for new, old in zip(readers, self.indexReaders)])

    @property
    def timestamp(self):
        return max(IndexReader(reader).timestamp for reader in self.indexReaders)


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
            version = util.Version.LATEST
        config = index.IndexWriterConfig(version, self.shared.analyzer(analyzer, version))
        config.openMode = index.IndexWriterConfig.OpenMode.values()['wra'.index(mode)]
        for name, value in attrs.items():
            setattr(config, name, value)
        self.policy = config.indexDeletionPolicy = index.SnapshotDeletionPolicy(config.indexDeletionPolicy)
        index.IndexWriter.__init__(self, self.shared.directory(directory), config)
        self.fields = {}

    def __del__(self):
        if hash(self):
            self.close()

    @classmethod
    def check(cls, directory, fix=False):
        "Check and optionally fix unlocked index, returning lucene CheckIndex.Status."
        with closing.store(directory) as directory:
            checkindex = index.CheckIndex(directory)
            lock = directory.makeLock(cls.WRITE_LOCK_NAME)
            assert lock.obtain(), "index must not be opened by any writer"
            with contextlib.closing(lock):
                status = checkindex.checkIndex()
                if fix:
                    checkindex.fixIndex(status)
        return status

    def set(self, name, cls=Field, **settings):
        """Assign settings to field name and return the field.
        
        :param name: registered name of field
        :param cls: optional `Field`_ constructor
        :param settings: stored, indexed, etc. options compatible with `Field`_
        """
        field = self.fields[name] = cls(name, **settings)
        return field

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
        """Atomically delete documents which match given term and add the new :meth:`document`.
        
        .. versionchanged:: 1.7 update in-place if only DocValues are given
        """
        doc = self.document(document, **terms)
        term = index.Term(name, *[value] if value else doc.getValues(name))
        fields = list(doc.iterator())
        if not all(field.fieldType().docValueType() for field in fields):
            self.updateDocument(term, doc)
        elif fields:
            self.updateDocValues(term, *fields)

    def delete(self, *query, **options):
        """Remove documents which match given query or term.
        
        :param query: :meth:`IndexSearcher.search` compatible query, or optimally a name and value
        :param options: additional :meth:`Analyzer.parse` options
        """
        parse = self.parse if len(query) == 1 else index.Term
        self.deleteDocuments(parse(*query, **options))

    def __iadd__(self, directory):
        "Add directory (or reader, searcher, writer) to index."
        with closing.store(getattr(directory, 'directory', directory)) as directory:
            self.addIndexes([directory])
        return self

    @contextlib.contextmanager
    def snapshot(self):
        """Return context manager of an index commit snapshot.
        
        .. versionchanged:: 1.4 lucene identifies snapshots by commit generation
        """
        commit = self.policy.snapshot()
        try:
            yield commit
        finally:
            self.policy.release(commit)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if any(args):
            self.rollback()
        else:
            self.commit()
        self.close()


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
    Assign custom settings or cache custom sorter for primary field if necessary.
    """
    def __init__(self, field, *args, **kwargs):
        Indexer.__init__(self, *args, **kwargs)
        self.field = field
        self.set(field, tokenized=False, omitNorms=True, indexOptions='docs_only')
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
        terms = set(sorter.terms(Query.alldocs().filter(cache=False), *readers))
        for filter, termsfilter in self.termsfilters.items():
            if terms:
                termsfilter.update(terms, op='andNot', cache=not self.nrt)
            if readers:
                termsfilter.update(sorter.terms(filter, *readers), cache=not self.nrt)
        self.indexSearcher = searcher
