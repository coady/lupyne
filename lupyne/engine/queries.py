"""
Query wrappers and search utilities.
"""

from future_builtins import filter, map
import itertools
import bisect
import heapq
import contextlib
import lucene
from java.lang import Integer
from java.util import Arrays, HashSet
from org.apache.lucene import index, search, util
from org.apache.lucene.search import highlight, spans, vectorhighlight
from org.apache.pylucene.queryparser.classic import PythonQueryParser
from ..utils import method


class Query(object):
    """Inherited lucene Query, with dynamic base class acquisition.

    Uses class methods and operator overloading for convenient query construction.
    """
    filter = method(search.QueryWrapperFilter)

    def __new__(cls, base, *args, **attrs):
        return base.__new__(type(base.__name__, (cls, base), {}))

    def __init__(self, base, *args, **attrs):
        base.__init__(self, *args)
        for name in attrs:
            setattr(self, name, attrs[name])

    @method
    def terms(self):
        """Generate set of query term items."""
        terms = HashSet().of_(index.Term)
        self.extractTerms(terms)
        return ((term.field(), term.text()) for term in terms)

    @classmethod
    def term(cls, name, value, **attrs):
        """Return lucene TermQuery."""
        return cls(search.TermQuery, index.Term(name, value), **attrs)

    @classmethod
    def boolean(cls, occur, *queries, **terms):
        self = search.BooleanQuery()
        for query in queries:
            self.add(query, occur)
        for name, values in terms.items():
            for value in ([values] if isinstance(values, basestring) else values):
                self.add(cls.term(name, value), occur)
        return self

    @classmethod
    def any(cls, *queries, **terms):
        """Return lucene BooleanQuery (OR) from queries and terms."""
        return cls.boolean(search.BooleanClause.Occur.SHOULD, *queries, **terms)

    @classmethod
    def all(cls, *queries, **terms):
        """Return lucene BooleanQuery (AND) from queries and terms."""
        return cls.boolean(search.BooleanClause.Occur.MUST, *queries, **terms)

    @classmethod
    def disjunct(cls, multiplier, *queries, **terms):
        """Return lucene DisjunctionMaxQuery from queries and terms."""
        self = cls(search.DisjunctionMaxQuery, Arrays.asList(queries), multiplier)
        for name, values in terms.items():
            for value in ([values] if isinstance(values, basestring) else values):
                self.add(cls.term(name, value))
        return self

    @classmethod
    def span(cls, *term):
        """Return `SpanQuery`_ from term name and value or a MultiTermQuery."""
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
        """Return lucene PrefixQuery."""
        return cls(search.PrefixQuery, index.Term(name, value))

    @classmethod
    def range(cls, name, start, stop, lower=True, upper=False):
        """Return lucene RangeQuery, by default with a half-open interval."""
        start, stop = (value if value is None else util.BytesRef(value) for value in (start, stop))
        return cls(search.TermRangeQuery, name, start, stop, lower, upper)

    @classmethod
    def phrase(cls, name, *values, **attrs):
        """Return lucene PhraseQuery.  None may be used as a placeholder."""
        self = cls(search.PhraseQuery, **attrs)
        for idx, value in enumerate(values):
            if value is not None:
                self.add(index.Term(name, value), idx)
        return self

    @classmethod
    def multiphrase(cls, name, *values):
        """Return lucene MultiPhraseQuery.  None may be used as a placeholder."""
        self = cls(search.MultiPhraseQuery)
        for idx, words in enumerate(values):
            if isinstance(words, basestring):
                words = [words]
            if words is not None:
                self.add([index.Term(name, word) for word in words], idx)
        return self

    @classmethod
    def wildcard(cls, name, value):
        """Return lucene WildcardQuery."""
        return cls(search.WildcardQuery, index.Term(name, value))

    @classmethod
    def fuzzy(cls, name, value, *args):
        """Return lucene FuzzyQuery."""
        return cls(search.FuzzyQuery, index.Term(name, value), *args)

    @classmethod
    def alldocs(cls):
        return cls(search.MatchAllDocsQuery)

    @classmethod
    def regexp(cls, name, value, *args):
        """Return lucene RegexpQuery."""
        return cls(search.RegexpQuery, index.Term(name, value), *args)

    def constant(self):
        """Return lucene ConstantScoreQuery."""
        return search.ConstantScoreQuery(self)

    def __pos__(self):
        """+self"""
        return Query.all(self)

    def __neg__(self):
        """-self"""
        return Query.boolean(search.BooleanClause.Occur.MUST_NOT, self)

    def __and__(self, other):
        """+self +other"""
        return Query.all(self, other)

    def __rand__(self, other):
        return Query.all(other, self)

    def __or__(self, other):
        """self other"""
        return Query.any(self, other)

    def __ror__(self, other):
        return Query.any(other, self)

    def __sub__(self, other):
        """self -other"""
        query = Query.any(self)
        query.add(other, search.BooleanClause.Occur.MUST_NOT)
        return query

    def __rsub__(self, other):
        query = Query.any(other)
        query.add(self, search.BooleanClause.Occur.MUST_NOT)
        return query


class SpanQuery(Query):
    """Inherited lucene SpanQuery with additional span constructors."""
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
        """Return lucene FieldMaskingSpanQuery, which allows combining span queries from different fields."""
        return SpanQuery(spans.FieldMaskingSpanQuery, self, name)

    def payload(self, *values):
        """Return lucene SpanPayloadCheckQuery from payload values."""
        base = spans.SpanNearPayloadCheckQuery if spans.SpanNearQuery.instance_(self) else spans.SpanPayloadCheckQuery
        return SpanQuery(base, self, Arrays.asList(list(map(lucene.JArray_byte, values))))


@contextlib.contextmanager
def suppress(exception):
    """Suppress specific lucene exception."""
    try:
        yield
    except lucene.JavaError as exc:
        if not exception.instance_(exc.getJavaException()):
            raise


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
    """Chained arrays with bisection lookup."""
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
    :param reverse: reverse flag used with sort
    """
    def __init__(self, name, type='string', reverse=False):
        type = self.typename = getattr(type, '__name__', type).capitalize()
        search.SortField.__init__(self, name, getattr(self.Type, type.upper()), reverse)

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
        """Return indexed values from default FieldCache using the given searcher."""
        assert not multi or self.typename == 'String'
        return Comparator(self.array(reader, multi) for reader in searcher.readers)


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
        """Return ordered suggested words for prefix."""
        start = bisect.bisect_left(self.words, prefix)
        stop = bisect.bisect_right(self.words, prefix + self.suffix, start)
        words = self.words[start:stop]
        if count is not None and count < len(words):
            return heapq.nlargest(count, words, key=self.__getitem__)
        words.sort(key=self.__getitem__, reverse=True)
        return words

    def edits(self, word, length=0):
        """Return set of potential words one edit distance away, mapped to valid prefix lengths."""
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
        """Generate ordered sets of words by increasing edit distance."""
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
        """Return term with text replaced as necessary."""
        field = term.field()
        for text in self.searcher.correct(field, term.text()):
            return index.Term(field, text)
        return term

    def rewrite(self, query):
        """Return term or phrase query with corrected terms substituted."""
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
