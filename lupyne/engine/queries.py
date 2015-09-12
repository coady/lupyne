"""
Query wrappers and search utilities.
"""

from future_builtins import filter, map
import itertools
import bisect
import heapq
import threading
import contextlib
import lucene
from java.lang import Integer
from java.util import Arrays, HashSet
from org.apache.lucene import index, queries, search, store, util
from org.apache.lucene.search import highlight, spans, vectorhighlight
from org.apache.pylucene import search as pysearch
from org.apache.pylucene.queryparser.classic import PythonQueryParser
from ..utils import method


class Query(object):
    """Inherited lucene Query, with dynamic base class acquisition.
    Uses class methods and operator overloading for convenient query construction.
    """
    def __new__(cls, base, *args, **attrs):
        return base.__new__(type(base.__name__, (cls, base), {}))

    def __init__(self, base, *args, **attrs):
        base.__init__(self, *args)
        for name in attrs:
            setattr(self, name, attrs[name])

    @method
    def filter(self, cache=True):
        "Return query as a filter, as specifically matching as possible, but defaulting to QueryWrapperFilter."
        if isinstance(self, search.PrefixQuery):
            filter = search.PrefixFilter(self.getPrefix())
        elif isinstance(self, search.TermRangeQuery):
            filter = search.TermRangeFilter(self.field, self.lowerTerm, self.upperTerm, self.includesLower(), self.includesUpper())
        elif isinstance(self, search.TermQuery):
            filter = queries.TermsFilter([self.getTerm()])
        elif isinstance(self, search.NumericRangeQuery):
            method = getattr(search.NumericRangeFilter, 'new{0}Range'.format(self.parameters_[0].__name__))
            filter = method(self.field, self.precisionStep, self.min, self.max, self.includesMin(), self.includesMax())
        else:
            filter = search.QueryWrapperFilter(self)
        return search.CachingWrapperFilter(filter) if cache else filter

    @method
    def terms(self):
        "Generate set of query term items."
        terms = HashSet().of_(index.Term)
        self.extractTerms(terms)
        return ((term.field(), term.text()) for term in terms)

    @classmethod
    def term(cls, name, value, **attrs):
        "Return lucene TermQuery."
        return cls(search.TermQuery, index.Term(name, value), **attrs)

    @classmethod
    def boolean(cls, occur, *queries, **terms):
        self = BooleanQuery(search.BooleanQuery)
        for query in queries:
            self.add(query, occur)
        for name, values in terms.items():
            for value in ([values] if isinstance(values, basestring) else values):
                self.add(cls.term(name, value), occur)
        return self

    @classmethod
    def any(cls, *queries, **terms):
        "Return `BooleanQuery`_ (OR) from queries and terms."
        return cls.boolean(search.BooleanClause.Occur.SHOULD, *queries, **terms)

    @classmethod
    def all(cls, *queries, **terms):
        "Return `BooleanQuery`_ (AND) from queries and terms."
        return cls.boolean(search.BooleanClause.Occur.MUST, *queries, **terms)

    @classmethod
    def disjunct(cls, multiplier, *queries, **terms):
        "Return lucene DisjunctionMaxQuery from queries and terms."
        self = cls(search.DisjunctionMaxQuery, Arrays.asList(queries), multiplier)
        for name, values in terms.items():
            for value in ([values] if isinstance(values, basestring) else values):
                self.add(cls.term(name, value))
        return self

    @classmethod
    def span(cls, *term):
        "Return `SpanQuery`_ from term name and value or a MultiTermQuery."
        if len(term) <= 1:
            return SpanQuery(spans.SpanMultiTermQueryWrapper, *term)
        return SpanQuery(spans.SpanTermQuery, index.Term(*term))

    @classmethod
    def near(cls, name, *values, **kwargs):
        """Return :meth:`SpanNearQuery <SpanQuery.near>` from terms.
        Term values which supply another field name will be masked."""
        spans = (cls.span(name, value) if isinstance(value, basestring) else cls.span(*value).mask(name) for value in values)
        return SpanQuery.near(*spans, **kwargs)

    @classmethod
    def prefix(cls, name, value):
        "Return lucene PrefixQuery."
        return cls(search.PrefixQuery, index.Term(name, value))

    @classmethod
    def range(cls, name, start, stop, lower=True, upper=False):
        "Return lucene RangeQuery, by default with a half-open interval."
        start, stop = (value if value is None else util.BytesRef(value) for value in (start, stop))
        return cls(search.TermRangeQuery, name, start, stop, lower, upper)

    @classmethod
    def phrase(cls, name, *values, **attrs):
        "Return lucene PhraseQuery.  None may be used as a placeholder."
        self = cls(search.PhraseQuery, **attrs)
        for idx, value in enumerate(values):
            if value is not None:
                self.add(index.Term(name, value), idx)
        return self

    @classmethod
    def multiphrase(cls, name, *values):
        "Return lucene MultiPhraseQuery.  None may be used as a placeholder."
        self = cls(search.MultiPhraseQuery)
        for idx, words in enumerate(values):
            if isinstance(words, basestring):
                words = [words]
            if words is not None:
                self.add([index.Term(name, word) for word in words], idx)
        return self

    @classmethod
    def wildcard(cls, name, value):
        "Return lucene WildcardQuery."
        return cls(search.WildcardQuery, index.Term(name, value))

    @classmethod
    def fuzzy(cls, name, value, *args):
        "Return lucene FuzzyQuery."
        return cls(search.FuzzyQuery, index.Term(name, value), *args)

    @classmethod
    def alldocs(cls):
        return cls(search.MatchAllDocsQuery)

    @classmethod
    def regexp(cls, name, value, *args):
        "Return lucene RegexpQuery."
        return cls(search.RegexpQuery, index.Term(name, value), *args)

    def constant(self):
        "Return lucene ConstantScoreQuery."
        return search.ConstantScoreQuery(self)

    def __pos__(self):
        return Query.all(self)
    def __neg__(self):
        return Query.boolean(search.BooleanClause.Occur.MUST_NOT, self)

    def __and__(self, other):
        return Query.all(self, other)
    def __rand__(self, other):
        return Query.all(other, self)

    def __or__(self, other):
        return Query.any(self, other)
    def __ror__(self, other):
        return Query.any(other, self)

    def __sub__(self, other):
        return Query.any(self).__isub__(other)
    def __rsub__(self, other):
        return Query.any(other).__isub__(self)


class BooleanQuery(Query):
    "Inherited lucene BooleanQuery with sequence interface to clauses."
    def __len__(self):
        return len(self.getClauses())

    def __iter__(self):
        return iter(self.getClauses())

    def __getitem__(self, index):
        return self.getClauses()[index]

    def __iand__(self, other):
        self.add(other, search.BooleanClause.Occur.MUST)
        return self

    def __ior__(self, other):
        self.add(other, search.BooleanClause.Occur.SHOULD)
        return self

    def __isub__(self, other):
        self.add(other, search.BooleanClause.Occur.MUST_NOT)
        return self


class SpanQuery(Query):
    "Inherited lucene SpanQuery with additional span constructors."
    def __getitem__(self, slc):
        start, stop, step = slc.indices(Integer.MAX_VALUE)
        assert step == 1, 'slice step is not supported'
        return SpanQuery(spans.SpanPositionRangeQuery, self, start, stop)

    def __sub__(self, other):
        return SpanQuery(spans.SpanNotQuery, self, other)

    def __or__(*spans_):
        return SpanQuery(spans.SpanOrQuery, spans_)

    def near(*spans_, **kwargs):
        """Return lucene SpanNearQuery from SpanQueries.
        
        :param slop: default 0
        :param inOrder: default True
        :param collectPayloads: default True
        """
        args = map(kwargs.get, ('slop', 'inOrder', 'collectPayloads'), (0, True, True))
        return SpanQuery(spans.SpanNearQuery, spans_, *args)

    def mask(self, name):
        "Return lucene FieldMaskingSpanQuery, which allows combining span queries from different fields."
        return SpanQuery(spans.FieldMaskingSpanQuery, self, name)

    def payload(self, *values):
        "Return lucene SpanPayloadCheckQuery from payload values."
        base = spans.SpanNearPayloadCheckQuery if spans.SpanNearQuery.instance_(self) else spans.SpanPayloadCheckQuery
        return SpanQuery(base, self, Arrays.asList(list(map(lucene.JArray_byte, values))))


class BooleanFilter(queries.BooleanFilter):
    "Inherited lucene BooleanFilter similar to BooleanQuery."
    def __init__(self, occur, *filters):
        queries.BooleanFilter.__init__(self)
        for filter in filters:
            self.add(queries.FilterClause(filter, occur))

    @classmethod
    def all(cls, *filters):
        "Return `BooleanFilter`_ (AND) from filters."
        return cls(search.BooleanClause.Occur.MUST, *filters)


class Filter(pysearch.PythonFilter):
    def getDocIdSet(self, context, acceptDocs):
        return util.FixedBitSet(context.reader().maxDoc())


@contextlib.contextmanager
def suppress(exception):
    "Suppress specific lucene exception."
    try:
        yield
    except lucene.JavaError as exc:
        if not exception.instance_(exc.getJavaException()):
            raise


class TermsFilter(search.CachingWrapperFilter):
    """Caching filter based on a unique field and set of matching values.
    Optimized for many terms and docs, with support for incremental updates.
    Suitable for searching external metadata associated with indexed identifiers.
    Call :meth:`refresh` to cache a new (or reopened) searcher.
    
    :param field: field name
    :param values: initial term values, synchronized with the cached filters
    """
    ops = {'or': 'update', 'and': 'intersection_update', 'andNot': 'difference_update'}

    def __init__(self, field, values=()):
        search.CachingWrapperFilter.__init__(self, Filter())
        self.field = field
        self.values = set(values)
        self.readers = set()
        self.lock = threading.Lock()

    def filter(self, values, cache=True):
        "Return lucene TermsFilter, optionally using the FieldCache."
        if cache:
            return search.FieldCacheTermsFilter(self.field, tuple(values))
        return queries.TermsFilter(self.field, tuple(map(util.BytesRef, values)))

    def apply(self, filter, op, readers):
        for reader in readers:
            with suppress(store.AlreadyClosedException):
                bitset = util.FixedBitSet.cast_(self.getDocIdSet(reader.context, None))
                docset = filter.getDocIdSet(reader.context, None)
                if docset:
                    getattr(bitset, op)(docset.iterator())

    def update(self, values, op='or', cache=True):
        """Update allowed values and corresponding cached bitsets.
        
        :param values: additional term values
        :param op: set operation used to combine terms and docs: *and*, *or*, *andNot*
        :param cache: optionally cache all term values using FieldCache
        """
        values = tuple(values)
        filter = self.filter(values, cache)
        with self.lock:
            self.apply(filter, op, self.readers)
            getattr(self.values, self.ops[op])(values)

    def refresh(self, searcher):
        "Refresh cached bitsets of current values for new segments of searcher."
        readers = set(searcher.readers)
        with self.lock:
            self.apply(self.filter(self.values), 'or', readers - self.readers)
            self.readers = {reader for reader in readers | self.readers if reader.refCount}

    def add(self, *values):
        "Add a few term values."
        self.update(values, cache=False)

    def discard(self, *values):
        "Discard a few term values."
        self.update(values, op='andNot', cache=False)


class Array(object):
    def __init__(self, array, size):
        self.array, self.size = array, size

    def __iter__(self):
        return map(self.__getitem__, xrange(self.size))

    def __getitem__(self, id):
        return self.array.get(id)


class TextArray(Array):
    def __getitem__(self, id):
        return self.array.get(id).utf8ToString()


class MultiArray(TextArray):
    def __getitem__(self, id):
        self.array.document = id
        return tuple(self.array.lookupOrd(id).utf8ToString() for id in iter(self.array.nextOrd, self.array.NO_MORE_ORDS))


class Comparator(object):
    "Chained arrays with bisection lookup."
    def __init__(self, arrays):
        self.arrays, self.offsets = list(arrays), [0]
        for array in self.arrays:
            self.offsets.append(len(self) + array.size)

    def __iter__(self):
        return itertools.chain.from_iterable(self.arrays)

    def __len__(self):
        return self.offsets[-1]

    def __getitem__(self, id):
        index = bisect.bisect_right(self.offsets, id) - 1
        return self.arrays[index][id - self.offsets[index]]


class SortField(search.SortField):
    """Inherited lucene SortField used for caching FieldCache parsers.
    
    :param name: field name
    :param type: type object or name compatible with SortField constants
    :param parser: lucene FieldCache.Parser or callable applied to field values
    :param reverse: reverse flag used with sort
    """
    def __init__(self, name, type='string', parser=None, reverse=False):
        type = self.typename = getattr(type, '__name__', type).capitalize()
        if parser is None:
            parser = getattr(self.Type, type.upper())
        elif not search.FieldCache.Parser.instance_(parser):
            base = getattr(pysearch, 'Python{}Parser'.format(type))
            namespace = {'parse' + type: staticmethod(parser), 'termsEnum': lambda self, terms: terms.iterator(None)}
            parser = object.__class__(base.__name__, (base,), namespace)()
        search.SortField.__init__(self, name, parser, reverse)

    def array(self, reader, multi=False):
        size = reader.maxDoc()
        if multi:
            return MultiArray(search.FieldCache.DEFAULT.getDocTermOrds(reader, self.field), size)
        if self.typename == 'String':
            return TextArray(search.FieldCache.DEFAULT.getTermsIndex(reader, self.field), size)
        if self.typename == 'Bytes':
            return TextArray(search.FieldCache.DEFAULT.getTerms(reader, self.field, True), size)
        method = getattr(search.FieldCache.DEFAULT, 'get{}s'.format(self.typename))
        return Array(method(reader, self.field, self.parser, False), size)

    def comparator(self, searcher, multi=False):
        "Return indexed values from default FieldCache using the given searcher."
        assert not multi or self.typename == 'String'
        return Comparator(self.array(reader, multi) for reader in searcher.readers)

    def filter(self, start, stop, lower=True, upper=False):
        "Return lucene FieldCacheRangeFilter based on field and type."
        if self.typename in ('String', 'Bytes'):
            return search.FieldCacheRangeFilter.newStringRange(self.field, start, stop, lower, upper)
        method = getattr(search.FieldCacheRangeFilter, 'new{}Range'.format(self.typename))
        return method(self.field, self.parser, start, stop, lower, upper)

    def terms(self, filter, *readers):
        "Generate field cache terms from docs which match filter from all segments."
        for reader in readers:
            array, docset = self.array(reader), filter.getDocIdSet(reader.context, reader.liveDocs)
            docsetit = docset.iterator() if docset else search.DocIdSetIterator.empty()
            for id in iter(docsetit.nextDoc, search.DocIdSetIterator.NO_MORE_DOCS):
                yield array[id]


class Highlighter(highlight.Highlighter):
    """Inherited lucene Highlighter with stored analysis options.
    
    :param searcher: `IndexSearcher`_ used for analysis, scoring, and optionally text retrieval
    :param query: lucene Query
    :param field: field name of text
    :param terms: highlight any matching term in query regardless of position
    :param fields: highlight matching terms from any field
    :param tag: optional html tag name
    :param formatter: optional lucene Formatter
    :param encoder: optional lucene Encoder
    """
    def __init__(self, searcher, query, field, terms=False, fields=False, tag='', formatter=None, encoder=None):
        if tag:
            formatter = highlight.SimpleHTMLFormatter('<{}>'.format(tag), '</{}>'.format(tag))
        scorer = (highlight.QueryTermScorer if terms else highlight.QueryScorer)(query, *(searcher.indexReader, field) * (not fields))
        highlight.Highlighter.__init__(self, *filter(None, [formatter, encoder, scorer]))
        self.searcher, self.field = searcher, field
        self.selector = HashSet(Arrays.asList([field]))

    def fragments(self, doc, count=1):
        """Return highlighted text fragments.
        
        :param doc: text string or doc id to be highlighted
        :param count: maximum number of fragments
        """
        if not isinstance(doc, basestring):
            doc = self.searcher.doc(doc, self.selector)[self.field]
        return doc and list(self.getBestFragments(self.searcher.analyzer, self.field, doc, count))


class FastVectorHighlighter(vectorhighlight.FastVectorHighlighter):
    """Inherited lucene FastVectorHighlighter with stored query.
    Fields must be stored and have term vectors with offsets and positions.
    
    :param searcher: `IndexSearcher`_ with stored term vectors
    :param query: lucene Query
    :param field: field name of text
    :param terms: highlight any matching term in query regardless of position
    :param fields: highlight matching terms from any field
    :param tag: optional html tag name
    :param fragListBuilder: optional lucene FragListBuilder
    :param fragmentsBuilder: optional lucene FragmentsBuilder
    """
    def __init__(self, searcher, query, field, terms=False, fields=False, tag='', fragListBuilder=None, fragmentsBuilder=None):
        if tag:
            fragmentsBuilder = vectorhighlight.SimpleFragmentsBuilder(['<{}>'.format(tag)], ['</{}>'.format(tag)])
        args = fragListBuilder or vectorhighlight.SimpleFragListBuilder(), fragmentsBuilder or vectorhighlight.SimpleFragmentsBuilder()
        vectorhighlight.FastVectorHighlighter.__init__(self, not terms, not fields, *args)
        self.searcher, self.field = searcher, field
        self.query = self.getFieldQuery(query)

    def fragments(self, id, count=1, size=100):
        """Return highlighted text fragments.
        
        :param id: document id
        :param count: maximum number of fragments
        :param size: maximum number of characters in fragment
        """
        return list(self.getBestFragments(self.query, self.searcher.indexReader, id, self.field, size, count))


class SpellChecker(dict):
    """Correct spellings and suggest words for queries.
    Supply a vocabulary mapping words to (reverse) sort keys, such as document frequencies.
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.words = sorted(self)
        self.alphabet = sorted(set(itertools.chain.from_iterable(self.words)))
        self.suffix = self.alphabet[-1] * max(map(len, self.words)) if self.alphabet else ''
        self.prefixes = {word[:stop] for word in self.words for stop in range(len(word) + 1)}

    def suggest(self, prefix, count=None):
        "Return ordered suggested words for prefix."
        start = bisect.bisect_left(self.words, prefix)
        stop = bisect.bisect_right(self.words, prefix + self.suffix, start)
        words = self.words[start:stop]
        if count is not None and count < len(words):
            return heapq.nlargest(count, words, key=self.__getitem__)
        words.sort(key=self.__getitem__, reverse=True)
        return words

    def edits(self, word, length=0):
        "Return set of potential words one edit distance away, mapped to valid prefix lengths."
        pairs = [(word[:index], word[index:]) for index in range(len(word) + 1)]
        deletes = (head + tail[1:] for head, tail in pairs[:-1])
        transposes = (head + tail[1::-1] + tail[2:] for head, tail in pairs[:-2])
        edits = {} if length else dict.fromkeys(itertools.chain(deletes, transposes), 0)
        for head, tail in pairs[length:]:
            if head not in self.prefixes:
                break
            for char in self.alphabet:
                prefix = head + char
                if prefix in self.prefixes:
                    edits[prefix + tail] = edits[prefix + tail[1:]] = len(prefix)
        return edits

    def correct(self, word):
        "Generate ordered sets of words by increasing edit distance."
        previous, edits = set(), {word: 0}
        for distance in range(len(word)):
            yield sorted(filter(self.__contains__, edits), key=self.__getitem__, reverse=True)
            previous.update(edits)
            groups = map(self.edits, edits, edits.values())
            edits = {edit: group[edit] for group in groups for edit in group if edit not in previous}


class SpellParser(PythonQueryParser):
    """Inherited lucene QueryParser which corrects spelling.
    Assign a searcher attribute or override :meth:`correct` implementation.
    """
    def correct(self, term):
        "Return term with text replaced as necessary."
        field = term.field()
        for text in self.searcher.correct(field, term.text()):
            return index.Term(field, text)
        return term

    def rewrite(self, query):
        "Return term or phrase query with corrected terms substituted."
        if search.TermQuery.instance_(query):
            term = search.TermQuery.cast_(query).term
            return search.TermQuery(self.correct(term))
        query = search.PhraseQuery.cast_(query)
        phrase = search.PhraseQuery()
        for position, term in zip(query.positions, query.terms):
            phrase.add(self.correct(term), position)
        return phrase

    def getFieldQuery_quoted(self, *args):
        return self.rewrite(self.getFieldQuery_quoted_super(*args))

    def getFieldQuery_slop(self, *args):
        return self.rewrite(self.getFieldQuery_slop_super(*args))
