import contextlib
import itertools
import operator
import os
from functools import partial
from typing import Iterator, Mapping, Optional
import lucene
from java.io import File, IOException, StringReader
from java.util import Arrays, HashSet
from org.apache.lucene import analysis, document, index, queries, search, store, util
from org.apache.lucene.queries import spans
from org.apache.lucene.search import spell, uhighlight
from .analyzers import Analyzer
from .queries import Query, DocValues, SpellParser
from .documents import Field, Document, Hits, GroupingSearch, Groups
from .utils import suppress, Atomic, SpellChecker


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
        if isinstance(directory, str):
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


class IndexReader:
    """Delegated lucene IndexReader, with a mapping interface of ids to document objects.

    Args:
        reader: lucene IndexReader
    """

    def __init__(self, reader):
        self.indexReader = reader

    def __getattr__(self, name):
        if name == 'indexReader':
            raise AttributeError(name)
        return getattr(index.DirectoryReader.cast_(self.indexReader), name)

    def __len__(self):
        return self.numDocs()

    def __contains__(self, id: int):
        bits = self.bits
        return (0 <= id < self.maxDoc()) and (not bits or bits.get(id))

    def __iter__(self) -> Iterator[int]:
        ids = range(self.maxDoc())
        bits = self.bits
        return filter(bits.get, ids) if bits else iter(ids)  # type: ignore

    @property
    def bits(self) -> util.Bits:
        return index.MultiBits.getLiveDocs(self.indexReader)

    @property
    def directory(self) -> store.Directory:
        """reader's lucene Directory"""
        return self.__getattr__('directory')()

    @property
    def path(self) -> str:
        """FSDirectory path"""
        return str(store.FSDirectory.cast_(self.directory).directory)

    @property
    def timestamp(self) -> float:
        """timestamp of reader's last commit"""
        return File(self.path, self.indexCommit.segmentsFileName).lastModified() * 0.001

    @property
    def readers(self) -> Iterator:
        """segment readers"""
        return (index.SegmentReader.cast_(context.reader()) for context in self.leaves())

    @property
    def segments(self) -> dict:
        """segment filenames with document counts"""
        return {reader.segmentName: reader.numDocs() for reader in self.readers}

    @property
    def fieldinfos(self) -> dict:
        """mapping of field names to lucene FieldInfos"""
        fieldinfos = index.FieldInfos.getMergedFieldInfos(self.indexReader)
        return {fieldinfo.name: fieldinfo for fieldinfo in fieldinfos.iterator()}

    def suggest(self, name: str, value, count: int = 1, **attrs) -> list:
        """Return spelling suggestions from DirectSpellChecker.

        Args:
            name: field name
            value: term
            count: maximum number of suggestions
            **attrs: DirectSpellChecker options
        """
        checker = spell.DirectSpellChecker()
        for attr in attrs:
            setattr(checker, attr, attrs[attr])
        return [word.string for word in checker.suggestSimilar(index.Term(name, value), count, self.indexReader)]

    def sortfield(self, name: str, type=None, reverse=False) -> search.SortField:
        """Return lucene SortField, deriving the the type from FieldInfos if necessary.

        Args:
            name: field name
            type: int, float, or name compatible with SortField constants
            reverse: reverse flag used with sort
        """
        if type is None:
            type = self.fieldinfos[name].docValuesType.toString()
        type = Field.types.get(type, type).upper()
        return search.SortField(name, getattr(search.SortField.Type, type), reverse)

    def docvalues(self, name: str, type=None) -> DocValues.Sorted:
        """Return chained lucene DocValues, suitable for custom sorting or grouping.

        Note multi-valued DocValues aren't thread-safe and only supported ordered iteration.

        Args:
            name: field name
            type: int or float for converting values
        """
        type = {int: int, float: util.NumericUtils.sortableLongToDouble}.get(type, util.BytesRef.utf8ToString)
        docValuesType = self.fieldinfos[name].docValuesType.toString().title().replace('_', '')
        method = getattr(index.MultiDocValues, f'get{docValuesType}Values')
        return getattr(DocValues, docValuesType)(method(self.indexReader, name), len(self), type)

    def copy(self, dest, query: search.Query = None, exclude: search.Query = None, merge: int = 0) -> int:
        """Copy the index to the destination directory.

        Optimized to use hard links if the destination is a file system path.

        Args:
            dest: destination directory path or lucene Directory
            query: optional lucene Query to select documents
            exclude: optional lucene Query to exclude documents
            merge: optionally merge into maximum number of segments
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

    def terms(self, name: str, value='', stop='', counts=False, distance=0, prefix=0) -> Iterator:
        """Generate a slice of term values, optionally with frequency counts.

        Args:
            name: field name
            value: term prefix, start value (given stop), or fuzzy value (given distance)
            stop: optional upper bound for range terms
            counts: include frequency counts
            distance: maximum edit distance for fuzzy terms
            prefix: prefix length for fuzzy terms
        """
        terms = index.MultiTerms.getTerms(self.indexReader, name)
        if not terms:
            return iter([])
        term, termsenum = index.Term(name, value), terms.iterator()
        if distance:
            terms = termsenum = search.FuzzyTermsEnum(terms, term, distance, prefix, False)
        else:
            termsenum.seekCeil(util.BytesRef(value))
            terms = itertools.chain([termsenum.term()], util.BytesRefIterator.cast_(termsenum))
        terms = map(operator.methodcaller('utf8ToString'), terms)
        predicate = partial(operator.gt, stop) if stop else operator.methodcaller('startswith', value)
        if not distance:
            terms = itertools.takewhile(predicate, terms)  # type: ignore
        return ((term, termsenum.docFreq()) for term in terms) if counts else terms

    def docs(self, name: str, value, counts=False) -> Iterator:
        """Generate doc ids which contain given term, optionally with frequency counts."""
        docsenum = index.MultiTerms.getTermPostingsEnum(self.indexReader, name, util.BytesRef(value))
        docs = iter(docsenum.nextDoc, index.PostingsEnum.NO_MORE_DOCS) if docsenum else ()
        return ((doc, docsenum.freq()) for doc in docs) if counts else iter(docs)  # type: ignore

    def positions(self, name: str, value, payloads=False, offsets=False) -> Iterator[tuple]:
        """Generate doc ids and positions which contain given term.

        Optionally with offsets, or only ones with payloads."""
        docsenum = index.MultiTerms.getTermPostingsEnum(self.indexReader, name, util.BytesRef(value))
        for doc in iter(docsenum.nextDoc, index.PostingsEnum.NO_MORE_DOCS) if docsenum else ():  # type: ignore
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

    def termvector(self, id: int, field: str, counts=False) -> Iterator:
        """Generate terms for given doc id and field, optionally with frequency counts."""
        termsenum, terms = self.vector(id, field)
        return ((term, int(termsenum.totalTermFreq())) for term in terms) if counts else terms

    def positionvector(self, id: int, field: str, offsets=False) -> Iterator[tuple]:
        """Generate terms and positions for given doc id and field, optionally with character offsets."""
        termsenum, terms = self.vector(id, field)
        for term in terms:
            docsenum = termsenum.postings(None)
            assert 0 <= docsenum.nextDoc() < docsenum.NO_MORE_DOCS
            positions = (docsenum.nextPosition() for _ in range(docsenum.freq()))
            if offsets:
                positions = ((docsenum.startOffset(), docsenum.endOffset()) for _ in positions)
            yield term, list(positions)

    def morelikethis(self, doc, *fields, **attrs) -> Query:
        """Return MoreLikeThis query for document.

        Args:
            doc: document id or text
            *fields: document fields to use, optional for termvectors
            **attrs: additional attributes to set on the morelikethis object
        """
        mlt = queries.mlt.MoreLikeThis(self.indexReader)
        mlt.fieldNames = fields or None
        for name, value in attrs.items():
            setattr(mlt, name, value)
        return mlt.like(fields[0], StringReader(doc)) if isinstance(doc, str) else mlt.like(doc)


class IndexSearcher(search.IndexSearcher, IndexReader):
    """Inherited lucene IndexSearcher, with a mixed-in IndexReader.

    Args:
        directory: directory path, lucene Directory, or lucene IndexReader
        analyzer: lucene Analyzer, default StandardAnalyzer
    """

    def __init__(self, directory, analyzer=None):
        self.shared = closing()
        super().__init__(self.shared.reader(directory))
        self.analyzer = self.shared.analyzer(analyzer)
        self.spellcheckers = {}

    def __del__(self):
        if hash(self):  # pragma: no branch
            self.decRef()

    def openIfChanged(self):
        return index.DirectoryReader.openIfChanged(index.DirectoryReader.cast_(self.indexReader))

    def reopen(self, spellcheckers=False) -> 'IndexSearcher':
        """Return current [IndexSearcher][lupyne.engine.indexers.IndexSearcher], only creating a new one if necessary.

        Args:
            spellcheckers: refresh cached :attr:`spellcheckers`
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

    def __getitem__(self, id: int) -> Document:
        return Document(self.doc(id))

    def get(self, id: int, *fields: str) -> Document:
        """Return [Document][lupyne.engine.documents.Document] with only selected fields loaded."""
        return Document(self.document(id, HashSet(Arrays.asList(fields))))

    def spans(self, query: spans.SpanQuery, positions=False) -> Iterator[tuple]:
        """Generate docs with occurrence counts for a span query.

        Args:
            query: lucene SpanQuery
            positions: optionally include slice positions instead of counts
        """
        offset = 0
        weight = query.createWeight(self, search.ScoreMode.COMPLETE_NO_SCORES, 1.0)
        postings = queries.spans.SpanWeight.Postings.POSITIONS
        for reader in self.readers:
            try:
                spans = weight.getSpans(reader.context, postings)
            except lucene.JavaError:  # EOF
                continue
            for doc in iter(spans.nextDoc, spans.NO_MORE_DOCS):  # type: int
                starts = iter(spans.nextStartPosition, spans.NO_MORE_POSITIONS)  # type: Iterator
                if positions:
                    values = [(start, spans.endPosition()) for start in starts]
                else:
                    values = sum(1 for _ in starts)  # type: ignore
                yield (doc + offset), values
            offset += reader.maxDoc()

    def parse(self, query, spellcheck=False, **kwargs):
        if isinstance(query, search.Query):
            return query
        if spellcheck:
            kwargs['parser'], kwargs['searcher'] = SpellParser, self
        return Analyzer.parse(self.analyzer, query, **kwargs)

    @property
    def highlighter(self) -> uhighlight.UnifiedHighlighter:
        """lucene UnifiedHighlighter"""
        return uhighlight.UnifiedHighlighter(self, self.analyzer)

    def count(self, *query, **options) -> int:
        """Return number of hits for given query or term.

        Args:
            *query: [search][lupyne.engine.indexers.IndexSearcher.search] compatible query, or optimally a name and value
            **options: additional [search][lupyne.engine.indexers.IndexSearcher.search] options
        """
        if len(query) > 1:
            return self.docFreq(index.Term(*query))
        query = self.parse(*query, **options) if query else Query.alldocs()
        return super().count(query)

    def collector(self, count=None, sort=None, reverse=False, scores=False, mincount=1000):
        if count is None:
            return search.CachingCollector.create(True, float('inf'))
        count = min(count, self.maxDoc() or 1)
        mincount = max(count, mincount)
        if sort is None:
            return search.TopScoreDocCollector.create(count, mincount)
        if isinstance(sort, str):
            sort = self.sortfield(sort, reverse=reverse)
        if not isinstance(sort, search.Sort):
            sort = search.Sort(sort)
        return search.TopFieldCollector.create(sort, count, mincount)

    def search(
        self, query=None, count=None, sort=None, reverse=False, scores=False, mincount=1000, timeout=None, **parser
    ) -> Hits:
        """Run query and return [Hits][lupyne.engine.documents.Hits].

        Note:
            changed in version 2.3: maxscore option removed; use Hits.maxscore property

        Args:
            query: query string or lucene Query
            count: maximum number of hits to retrieve
            sort: lucene Sort parameters
            reverse: reverse flag used with sort
            scores: compute scores for candidate results when sorting
            mincount: total hit count accuracy threshold
            timeout: stop search after elapsed number of seconds
            **parser: [parse][lupyne.engine.analyzers.Analyzer.parse]` options
        """
        query = Query.alldocs() if query is None else self.parse(query, **parser)
        results = cache = collector = self.collector(count, sort, reverse, scores, mincount)
        counter = search.TimeLimitingCollector.getGlobalCounter()
        if timeout is not None:
            results = search.TimeLimitingCollector(collector, counter, int(timeout * 1000))
        with suppress(search.TimeLimitingCollector.TimeExceededException):
            super().search(query, results)
            timeout = None
        if isinstance(cache, search.CachingCollector):
            collector = search.TotalHitCountCollector()
            cache.replay(collector)
            count = collector.totalHits or 1
            collector = self.collector(count, sort, reverse, scores, count)
            cache.replay(collector)
        topdocs = collector.topDocs()
        if scores:
            search.TopFieldCollector.populateScores(topdocs.scoreDocs, self, query)
        return Hits(self, topdocs.scoreDocs, topdocs.totalHits)

    def facets(self, query, *fields: str, **query_map: dict) -> dict:
        """Return mapping of document counts for the intersection with each facet.

        Args:
            query: query string or lucene Query
            *fields: field names for lucene GroupingSearch
            **query_map: `{facet: {key: query, ...}, ...}` for intersected query counts
        """
        query = self.parse(query)
        counts = {field: self.groupby(field, query).facets for field in fields}
        for facet, queries in query_map.items():
            counts[facet] = {key: self.count(Query.all(query, queries[key])) for key in queries}
        return counts

    def groupby(self, field: str, query, count: Optional[int] = None, start: int = 0, **attrs) -> Groups:
        """Return [Hits][lupyne.engine.documents.Hits] grouped by field
        using a [GroupingSearch][lupyne.engine.documents.GroupingSearch]."""
        return GroupingSearch(field, **attrs).search(self, self.parse(query), count, start)

    def spellchecker(self, field: str) -> SpellChecker:
        """Return and cache spellchecker for given field."""
        try:
            return self.spellcheckers[field]
        except KeyError:
            return self.spellcheckers.setdefault(field, SpellChecker(self.terms(field, counts=True)))

    def complete(self, field: str, prefix: str, count: Optional[int] = None) -> list:
        """Return ordered suggested words for prefix."""
        return self.spellchecker(field).complete(prefix, count)

    def match(self, document: Mapping, *queries) -> Iterator[float]:
        """Generate scores for all queries against a given document mapping."""
        searcher = index.memory.MemoryIndex()
        for name, value in document.items():
            args = [self.analyzer] * isinstance(value, str)
            searcher.addField(name, value, *args)
        return (searcher.search(self.parse(query)) for query in queries)


class MultiSearcher(IndexSearcher):
    """IndexSearcher with underlying lucene MultiReader.

    Args:
        reader: directory paths, Directories, IndexReaders, or a single MultiReader
        analyzer: lucene Analyzer, default StandardAnalyzer
    """

    def __init__(self, reader, analyzer=None):
        super().__init__(reader, analyzer)
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

    Args:
        directory: directory path or lucene Directory
        mode: file mode (rwa), except updating (+) is implied
        analyzer: lucene Analyzer, default StandardAnalyzer
        version: lucene Version argument passed to IndexWriterConfig, default is latest
        **attrs: additional attributes to set on IndexWriterConfig
    """

    parse = IndexSearcher.parse

    def __init__(self, directory, mode: str = 'a', analyzer=None, version=None, **attrs):
        self.shared = closing()
        args = [] if analyzer is None else [self.shared.analyzer(analyzer)]
        config = index.IndexWriterConfig(*args)
        config.openMode = index.IndexWriterConfig.OpenMode.values()['wra'.index(mode)]
        for name, value in attrs.items():
            setattr(config, name, value)
        self.policy = config.indexDeletionPolicy = index.SnapshotDeletionPolicy(config.indexDeletionPolicy)
        super().__init__(self.shared.directory(directory), config)
        self.fields = {}  # type: dict

    def __del__(self):
        if hash(self):
            with suppress(IOException):
                self.close()

    def __len__(self):
        return self.docStats.numDocs

    @classmethod
    def check(cls, directory, repair=False) -> index.CheckIndex.Status:
        """Check and optionally fix unlocked index, returning lucene CheckIndex.Status."""
        with closing.store(directory) as directory:
            with contextlib.closing(index.CheckIndex(directory)) as checkindex:
                status = checkindex.checkIndex()
                if repair:
                    checkindex.exorciseIndex(status)
        return status

    def set(self, name: str, cls=Field, **settings) -> Field:
        """Assign settings to field name and return the field.

        Args:
            name: registered name of field
            cls: optional [Field][lupyne.engine.documents.Field] constructor
            **settings: stored, indexed, etc. options compatible with [Field][lupyne.engine.documents.Field]
        """
        field = self.fields[name] = cls(name, **settings)
        return field

    def document(self, items=(), **terms) -> document.Document:
        """Return lucene Document from mapping of field names to one or multiple values."""
        doc = document.Document()
        for name, values in dict(items, **terms).items():
            if isinstance(values, Atomic):
                values = (values,)
            for field in self.fields[name].items(*values):
                doc.add(field)
        return doc

    def add(self, document=(), **terms):
        """Add [document][lupyne.engine.indexers.IndexWriter.document] to index with optional boost."""
        self.addDocument(self.document(document, **terms))

    def update(self, name: str, value='', document=(), **terms):
        """Atomically delete documents which match given term
        and add the new [document][lupyne.engine.indexers.IndexWriter.document]."""
        doc = self.document(document, **terms)
        term = index.Term(name, *[value] if value else doc.getValues(name))
        fields = list(doc.iterator())
        types = [Field.cast_(field.fieldType()) for field in fields]
        noindex = index.IndexOptions.NONE
        if any(ft.stored() or ft.indexOptions() != noindex or Field.dimensions.fget(ft) for ft in types):
            self.updateDocument(term, doc)
        elif fields:
            self.updateDocValues(term, *fields)

    def delete(self, *query, **options):
        """Remove documents which match given query or term.

        Args:
            *query: [search][lupyne.engine.indexers.IndexSearcher.search] compatible query, or optimally a name and value
            **options: additional [parse][lupyne.engine.analyzers.Analyzer.parse] options
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
        """Return context manager of an index commit snapshot."""
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

    Creates an [IndexWriter][lupyne.engine.indexers.IndexWriter]
    with a delegated [IndexSearcher][lupyne.engine.indexers.IndexSearcher].

    Args:
        nrt: optionally use a near real-time searcher
    """

    def __init__(self, directory, mode='a', analyzer=None, version=None, nrt=False, **attrs):
        super().__init__(directory, mode, analyzer, version, **attrs)
        super().commit()
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
        """Store refreshed searcher with [reopen][lupyne.engine.indexers.IndexSearcher.reopen] caches."""
        self.indexSearcher = self.indexSearcher.reopen(**caches)

    def commit(self, merge=False, **caches):
        """Commit writes and [refresh][lupyne.engine.indexers.Indexer.refresh] searcher.

        Args:
            merge: merge segments with deletes, or optionally specify maximum number of segments
        """
        super().commit()
        if merge:
            if isinstance(merge, bool):
                self.forceMergeDeletes()
            else:
                self.forceMerge(merge)
            super().commit()
        self.refresh(**caches)
