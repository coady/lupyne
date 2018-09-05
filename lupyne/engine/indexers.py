import contextlib
import itertools
import operator
import os
from functools import partial
import lucene
from java.io import File, IOException, StringReader
from java.util import Arrays, HashSet
from org.apache.lucene import analysis, document, index, queries, search, store, util
from org.apache.lucene.search import spell, uhighlight
from six import string_types
from six.moves import filter, map, range, zip
from .analyzers import Analyzer
from .queries import Query, DocValues, SpellParser
from .documents import Field, Document, Hits, GroupingSearch
from .utils import long, suppress, Atomic, SpellChecker


class closing(set):
    """Manage lifespan of registered objects, similar to contextlib.closing."""
    def __del__(self):
        for obj in self:
            obj.close()

    def analyzer(self, analyzer):
        if analyzer is None:
            analyzer = analysis.standard.StandardAnalyzer()
            self.add(analyzer)
        return analyzer

    def directory(self, directory):
        if directory is None:
            directory = store.RAMDirectory()
            self.add(directory)
        elif isinstance(directory, string_types):
            directory = store.FSDirectory.open(File(directory).toPath())
            self.add(directory)
        return directory

    def reader(self, reader):
        if isinstance(reader, index.IndexReader):
            reader.incRef()
        elif isinstance(reader, index.IndexWriter):
            reader = index.DirectoryReader.open(reader)
        elif isinstance(reader, Atomic):
            reader = index.DirectoryReader.open(self.directory(reader))
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
            dest.copyFrom(commit.directory, filename, filename, store.IOContext.DEFAULT)
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
        ids = range(self.maxDoc())
        bits = self.bits
        return filter(bits.get, ids) if bits else iter(ids)

    @property
    def bits(self):
        return index.MultiFields.getLiveDocs(self.indexReader)

    @property
    def directory(self):
        """reader's lucene Directory"""
        return self.__getattr__('directory')()

    @property
    def path(self):
        """FSDirectory path"""
        return str(store.FSDirectory.cast_(self.directory).directory)

    @property
    def timestamp(self):
        """timestamp of reader's last commit"""
        return File(self.path, self.indexCommit.segmentsFileName).lastModified() * 0.001

    @property
    def readers(self):
        """segment readers"""
        return (index.SegmentReader.cast_(context.reader()) for context in self.leaves())

    @property
    def segments(self):
        """segment filenames with document counts"""
        return {reader.segmentName: reader.numDocs() for reader in self.readers}

    @property
    def fieldinfos(self):
        """mapping of field names to lucene FieldInfos"""
        fieldinfos = index.MultiFields.getMergedFieldInfos(self.indexReader)
        return {fieldinfo.name: fieldinfo for fieldinfo in fieldinfos.iterator()}

    def suggest(self, name, value, count=1, **attrs):
        """Return spelling suggestions from DirectSpellChecker.

        :param name: field name
        :param value: term
        :param count: maximum number of suggestions
        :param attrs: DirectSpellChecker options
        """
        checker = spell.DirectSpellChecker()
        for attr in attrs:
            setattr(checker, attr, attrs[attr])
        return [word.string for word in checker.suggestSimilar(index.Term(name, value), count, self.indexReader)]

    def sortfield(self, name, type=None, reverse=False):
        """Return lucene SortField, deriving the the type from FieldInfos if necessary.

        :param name: field name
        :param type: int, float, or name compatible with SortField constants
        :param reverse: reverse flag used with sort
        """
        if type is None:
            type = self.fieldinfos[name].docValuesType.toString()
        type = Field.types.get(type, type).upper()
        return search.SortField(name, getattr(search.SortField.Type, type), reverse)

    def docvalues(self, name, type=None):
        """Return chained lucene DocValues, suitable for custom sorting or grouping.

        Note multi-valued DocValues aren't thread-safe and only supported ordered iteration.

        :param name: field name
        :param type: int or float for converting values
        """
        type = {int: int, float: util.NumericUtils.sortableLongToDouble}.get(type, util.BytesRef.utf8ToString)
        docValuesType = self.fieldinfos[name].docValuesType.toString().title().replace('_', '')
        method = getattr(index.MultiDocValues, 'get{}Values'.format(docValuesType))
        return getattr(DocValues, docValuesType)(method(self.indexReader, name), len(self), type)

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

    def terms(self, name, value='', stop='', counts=False, distance=0, prefix=0):
        """Generate a slice of term values, optionally with frequency counts.

        :param name: field name
        :param value: term prefix, start value (given stop), or fuzzy value (given distance)
        :param stop: optional upper bound for range terms
        :param counts: include frequency counts
        :param distance: maximum edit distance for fuzzy terms
        :param prefix: prefix length for fuzzy terms
        """
        terms = index.MultiFields.getTerms(self.indexReader, name)
        if not terms:
            return iter([])
        term, termsenum = index.Term(name, value), terms.iterator()
        if distance:
            terms = termsenum = search.FuzzyTermsEnum(terms, util.AttributeSource(), term, distance, prefix, False)
        else:
            termsenum.seekCeil(util.BytesRef(value))
            terms = itertools.chain([termsenum.term()], util.BytesRefIterator.cast_(termsenum))
        terms = map(operator.methodcaller('utf8ToString'), terms)
        predicate = partial(operator.gt, stop) if stop else operator.methodcaller('startswith', value)
        if not distance:
            terms = itertools.takewhile(predicate, terms)
        return ((term, termsenum.docFreq()) for term in terms) if counts else terms

    def docs(self, name, value, counts=False):
        """Generate doc ids which contain given term, optionally with frequency counts."""
        docsenum = index.MultiFields.getTermDocsEnum(self.indexReader, name, util.BytesRef(value))
        docs = iter(docsenum.nextDoc, index.PostingsEnum.NO_MORE_DOCS) if docsenum else ()
        return ((doc, docsenum.freq()) for doc in docs) if counts else iter(docs)

    def positions(self, name, value, payloads=False, offsets=False):
        """Generate doc ids and positions which contain given term, optionally with offsets, or only ones with payloads."""
        docsenum = index.MultiFields.getTermPositionsEnum(self.indexReader, name, util.BytesRef(value))
        for doc in (iter(docsenum.nextDoc, index.PostingsEnum.NO_MORE_DOCS) if docsenum else ()):
            positions = (docsenum.nextPosition() for _ in range(docsenum.freq()))
            if payloads:
                positions = ((position, docsenum.payload.utf8ToString()) for position in positions if docsenum.payload)
            elif offsets:
                positions = ((docsenum.startOffset(), docsenum.endOffset()) for position in positions)
            yield doc, list(positions)

    def vector(self, id, field):
        terms = self.getTermVector(id, field)
        termsenum = terms.iterator() if terms else index.TermsEnum.EMPTY
        return termsenum, map(operator.methodcaller('utf8ToString'), util.BytesRefIterator.cast_(termsenum))

    def termvector(self, id, field, counts=False):
        """Generate terms for given doc id and field, optionally with frequency counts."""
        termsenum, terms = self.vector(id, field)
        return ((term, int(termsenum.totalTermFreq())) for term in terms) if counts else terms

    def positionvector(self, id, field, offsets=False):
        """Generate terms and positions for given doc id and field, optionally with character offsets."""
        termsenum, terms = self.vector(id, field)
        for term in terms:
            docsenum = termsenum.postings(None)
            assert 0 <= docsenum.nextDoc() < docsenum.NO_MORE_DOCS
            positions = (docsenum.nextPosition() for _ in range(docsenum.freq()))
            if offsets:
                positions = ((docsenum.startOffset(), docsenum.endOffset()) for _ in positions)
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
        return mlt.like(fields[0], StringReader(doc)) if isinstance(doc, string_types) else mlt.like(doc)


class IndexSearcher(search.IndexSearcher, IndexReader):
    """Inherited lucene IndexSearcher, with a mixed-in IndexReader.

    :param directory: directory path, lucene Directory, or lucene IndexReader
    :param analyzer: lucene Analyzer, default StandardAnalyzer
    """
    def __init__(self, directory, analyzer=None):
        self.shared = closing()
        search.IndexSearcher.__init__(self, self.shared.reader(directory))
        self.analyzer = self.shared.analyzer(analyzer)
        self.spellcheckers = {}

    @classmethod
    def load(cls, directory, analyzer=None):
        """Open `IndexSearcher`_ with a lucene RAMDirectory, loading index into memory."""
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

    def reopen(self, spellcheckers=False):
        """Return current `IndexSearcher`_, only creating a new one if necessary.

        :param spellcheckers: refresh cached :attr:`spellcheckers`
        """
        reader = self.openIfChanged()
        if reader is None:
            return self
        other = type(self)(reader, self.analyzer)
        other.decRef()
        other.shared = self.shared
        if spellcheckers:
            for field in self.spellcheckers:
                other.spellchecker(field)
        else:
            other.spellcheckers = dict(self.spellcheckers)
        return other

    def __getitem__(self, id):
        return Document(self.doc(id))

    def get(self, id, *fields):
        """Return `Document`_ with only selected fields loaded."""
        return Document(self.document(id, HashSet(Arrays.asList(fields))))

    def spans(self, query, positions=False):
        """Generate docs with occurrence counts for a span query.

        :param query: lucene SpanQuery
        :param positions: optionally include slice positions instead of counts
        """
        offset = 0
        weight = query.createWeight(self, False, 1.0)
        postings = search.spans.SpanWeight.Postings.POSITIONS
        for reader in self.readers:
            try:
                spans = weight.getSpans(reader.context, postings)
            except lucene.JavaError:  # EOF
                continue
            for doc in iter(spans.nextDoc, spans.NO_MORE_DOCS):
                starts = iter(spans.nextStartPosition, spans.NO_MORE_POSITIONS)
                if positions:
                    values = [(start, spans.endPosition()) for start in starts]
                else:
                    values = sum(1 for _ in starts)
                yield (doc + offset), values
            offset += reader.maxDoc()

    def parse(self, query, spellcheck=False, **kwargs):
        if isinstance(query, search.Query):
            return query
        if spellcheck:
            kwargs['parser'], kwargs['searcher'] = SpellParser, self
        return Analyzer.parse(self.analyzer, query, **kwargs)

    @property
    def highlighter(self):
        """lucene UnifiedHighlighter"""
        return uhighlight.UnifiedHighlighter(self, self.analyzer)

    def count(self, *query, **options):
        """Return number of hits for given query or term.

        :param query: :meth:`search` compatible query, or optimally a name and value
        :param options: additional :meth:`search` options
        """
        if len(query) > 1:
            return self.docFreq(index.Term(*query))
        query = self.parse(*query, **options) if query else Query.alldocs()
        return super(IndexSearcher, self).count(query)

    def collector(self, count=None, sort=None, reverse=False, scores=False, maxscore=False):
        if count is None:
            return search.CachingCollector.create(True, float('inf'))
        count = min(count, self.maxDoc() or 1)
        if sort is None:
            return search.TopScoreDocCollector.create(count)
        if isinstance(sort, string_types):
            sort = self.sortfield(sort, reverse=reverse)
        if not isinstance(sort, search.Sort):
            sort = search.Sort(sort)
        return search.TopFieldCollector.create(sort, count, True, scores, maxscore)

    def search(self, query=None, count=None, sort=None, reverse=False, scores=False, maxscore=False, timeout=None, **parser):
        """Run query and return `Hits`_.

        .. versionchanged:: 1.4 sort param for lucene only;  use Hits.sorted with a callable

        :param query: query string or lucene Query
        :param count: maximum number of hits to retrieve
        :param sort: lucene Sort parameters
        :param reverse: reverse flag used with sort
        :param scores: compute scores for candidate results when sorting
        :param maxscore: compute maximum score of all results when sorting
        :param timeout: stop search after elapsed number of seconds
        :param parser: :meth:`Analyzer.parse` options
        """
        query = Query.alldocs() if query is None else self.parse(query, **parser)
        cache = collector = self.collector(count, sort, reverse, scores, maxscore)
        counter = search.TimeLimitingCollector.getGlobalCounter()
        results = collector if timeout is None else search.TimeLimitingCollector(collector, counter, long(timeout * 1000))
        with suppress(search.TimeLimitingCollector.TimeExceededException):
            search.IndexSearcher.search(self, query, results)
            timeout = None
        if isinstance(cache, search.CachingCollector):
            collector = search.TotalHitCountCollector()
            cache.replay(collector)
            collector = self.collector(collector.totalHits or 1, sort, reverse, scores, maxscore)
            cache.replay(collector)
        topdocs = collector.topDocs()
        stats = (topdocs.totalHits, topdocs.maxScore) * (timeout is None)
        return Hits(self, topdocs.scoreDocs, *stats)

    def facets(self, query, *fields, **query_map):
        """Return mapping of document counts for the intersection with each facet.

        .. versionchanged:: 1.6 filters are no longer implicitly cached, a `GroupingSearch`_ is used instead

        :param query: query string or lucene Query
        :param fields: field names for lucene GroupingSearch
        :param query_map: `{facet: {key: query, ...}, ...}` for intersected query counts
        """
        query = self.parse(query)
        counts = {field: self.groupby(field, query).facets for field in fields}
        for facet, queries in query_map.items():
            counts[facet] = {key: self.count(Query.all(query, queries[key])) for key in queries}
        return counts

    def groupby(self, field, query, count=None, start=0, **attrs):
        """Return `Hits`_ grouped by field using a `GroupingSearch`_."""
        return GroupingSearch(field, **attrs).search(self, self.parse(query), count, start)

    def spellchecker(self, field):
        """Return and cache spellchecker for given field."""
        try:
            return self.spellcheckers[field]
        except KeyError:
            return self.spellcheckers.setdefault(field, SpellChecker(self.terms(field, counts=True)))

    def complete(self, field, prefix, count=None):
        """Return ordered suggested words for prefix."""
        return self.spellchecker(field).complete(prefix, count)

    def match(self, document, *queries):
        """Generate scores for all queries against a given document mapping."""
        searcher = index.memory.MemoryIndex()
        for name, value in document.items():
            args = [self.analyzer] * isinstance(value, string_types)
            searcher.addField(name, value, *args)
        return (searcher.search(self.parse(query)) for query in queries)


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
    :param version: lucene Version argument passed to IndexWriterConfig, default is latest
    :param attrs: additional attributes to set on IndexWriterConfig
    """
    __len__ = index.IndexWriter.numDocs
    parse = IndexSearcher.__dict__['parse']

    def __init__(self, directory=None, mode='a', analyzer=None, version=None, **attrs):
        self.shared = closing()
        config = index.IndexWriterConfig() if analyzer is None else index.IndexWriterConfig(self.shared.analyzer(analyzer))
        config.openMode = index.IndexWriterConfig.OpenMode.values()['wra'.index(mode)]
        for name, value in attrs.items():
            setattr(config, name, value)
        self.policy = config.indexDeletionPolicy = index.SnapshotDeletionPolicy(config.indexDeletionPolicy)
        index.IndexWriter.__init__(self, self.shared.directory(directory), config)
        self.fields = {}

    def __del__(self):
        if hash(self):
            with suppress(IOException):
                self.close()

    @classmethod
    def check(cls, directory, fix=False):
        """Check and optionally fix unlocked index, returning lucene CheckIndex.Status."""
        with closing.store(directory) as directory:
            with contextlib.closing(index.CheckIndex(directory)) as checkindex:
                status = checkindex.checkIndex()
                if fix:
                    checkindex.exorciseIndex(status)
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
        """Return lucene Document from mapping of field names to one or multiple values."""
        doc = document.Document()
        for name, values in dict(items, **terms).items():
            if isinstance(values, Atomic):
                values = values,
            for field in self.fields[name].items(*values):
                doc.add(field)
        return doc

    def add(self, document=(), **terms):
        """Add :meth:`document` to index with optional boost."""
        self.addDocument(self.document(document, **terms))

    def update(self, name, value='', document=(), **terms):
        """Atomically delete documents which match given term and add the new :meth:`document`.

        .. versionchanged:: 1.7 update in-place if only DocValues are given
        """
        doc = self.document(document, **terms)
        term = index.Term(name, *[value] if value else doc.getValues(name))
        fields = list(doc.iterator())
        types = [Field.cast_(field.fieldType()) for field in fields]
        if any(type.stored() or type.indexOptions() != index.IndexOptions.NONE or type.pointDimensionCount() for type in types):
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
        """Add directory (or reader, searcher, writer) to index."""
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
        """Store refreshed searcher with :meth:`IndexSearcher.reopen` caches."""
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
