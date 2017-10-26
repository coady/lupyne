"""
Query wrappers and search utilities.
"""

import contextlib
import lucene
from java.lang import Integer
from java.util import Arrays, HashSet
from org.apache.lucene import index, search, util
from org.apache.lucene.search import highlight, spans, vectorhighlight
from org.apache.pylucene.queryparser.classic import PythonQueryParser
from six import string_types
from six.moves import filter, map, range
from ..utils import method


class Query(object):
    """Inherited lucene Query, with dynamic base class acquisition.

    Uses class methods and operator overloading for convenient query construction.
    """
    def __new__(cls, base, *args):
        return base.__new__(type(base.__name__, (cls, base), {}))

    def __init__(self, base, *args):
        base.__init__(self, *args)

    @classmethod
    def term(cls, name, value):
        """Return lucene TermQuery."""
        return cls(search.TermQuery, index.Term(name, value))

    @classmethod
    def terms(cls, name, values):
        """Return lucene TermInSetQuery, optimizing a SHOULD BooleanQuery of many terms."""
        return cls(search.TermInSetQuery, name, list(map(util.BytesRef, values)))

    @classmethod
    def boolean(cls, occur, *queries, **terms):
        builder = search.BooleanQuery.Builder()
        for query in queries:
            builder.add(query, occur)
        for name, values in terms.items():
            for value in ([values] if isinstance(values, string_types) else values):
                builder.add(cls.term(name, value), occur)
        return builder.build()

    @classmethod
    def any(cls, *queries, **terms):
        """Return lucene BooleanQuery with SHOULD clauses from queries and terms."""
        return cls.boolean(search.BooleanClause.Occur.SHOULD, *queries, **terms)

    @classmethod
    def all(cls, *queries, **terms):
        """Return lucene BooleanQuery with MUST clauses from queries and terms."""
        return cls.boolean(search.BooleanClause.Occur.MUST, *queries, **terms)

    @classmethod
    def filter(cls, *queries, **terms):
        """Return lucene BooleanQuery with FILTER clauses from queries and terms."""
        return cls.boolean(search.BooleanClause.Occur.FILTER, *queries, **terms)

    @classmethod
    def disjunct(cls, multiplier, *queries, **terms):
        """Return lucene DisjunctionMaxQuery from queries and terms."""
        terms = tuple(cls.term(name, value) for name, values in terms.items()
                      for value in ([values] if isinstance(values, string_types) else values))
        return cls(search.DisjunctionMaxQuery, Arrays.asList(queries + terms), multiplier)

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
        spans = (cls.span(name, value) if isinstance(value, string_types) else cls.span(*value).mask(name) for value in values)
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
        """Return lucene MultiPhraseQuery.  None may be used as a placeholder."""
        builder = search.MultiPhraseQuery.Builder()
        for attr in attrs:
            setattr(builder, attr, attrs[attr])
        for idx, words in enumerate(values):
            if isinstance(words, string_types):
                words = [words]
            if words is not None:
                builder.add([index.Term(name, word) for word in words], idx)
        return builder.build()

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
        """Return lucene MatchAllDocsQuery."""
        return cls(search.MatchAllDocsQuery)

    @classmethod
    def nodocs(cls):
        """Return lucene MatchNoDocsQuery."""
        return cls(search.MatchNoDocsQuery)

    @classmethod
    def regexp(cls, name, value, *args):
        """Return lucene RegexpQuery."""
        return cls(search.RegexpQuery, index.Term(name, value), *args)

    def constant(self):
        """Return lucene ConstantScoreQuery."""
        return Query(search.ConstantScoreQuery, self)

    def boost(self, value):
        """Return lucene BoostQuery."""
        return Query(search.BoostQuery, self, value)

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

    @method
    def __sub__(self, other):
        """self -other"""
        builder = search.BooleanQuery.Builder()
        builder.add(self, search.BooleanClause.Occur.SHOULD)
        builder.add(other, search.BooleanClause.Occur.MUST_NOT)
        return builder.build()

    def __rsub__(self, other):
        return Query.__sub__(other, self)


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
        """
        args = map(kwargs.get, ('slop', 'inOrder'), (0, True))
        return SpanQuery(spans.SpanNearQuery, spans_, *args)

    def mask(self, name):
        """Return lucene FieldMaskingSpanQuery, which allows combining span queries from different fields."""
        return SpanQuery(spans.FieldMaskingSpanQuery, self, name)

    def boost(self, value):
        """Return lucene SpanBoostQuery."""
        return SpanQuery(spans.SpanBoostQuery, self, value)

    def containing(self, other):
        """Return lucene SpanContainingQuery."""
        return SpanQuery(spans.SpanContainingQuery, self, other)

    def within(self, other):
        """Return lucene SpanWithinQuery."""
        return SpanQuery(spans.SpanWithinQuery, self, other)


@contextlib.contextmanager
def suppress(exception):
    """Suppress specific lucene exception."""
    try:
        yield
    except lucene.JavaError as exc:
        if not exception.instance_(exc.getJavaException()):
            raise


class DocValues:
    """DocValues with type conversion."""
    class Numeric(object):
        def __init__(self, docvalues, size, type):
            self.docvalues, self.size, self.type = docvalues, size, type

        def __iter__(self):
            return map(self.__getitem__, range(self.size))

        def __getitem__(self, id):
            return self.type(self.docvalues.get(id))

        def select(self, ids):
            """Return mapping of doc ids to values."""
            return {id: self[id] for id in sorted(ids)}

    Binary = Sorted = Numeric

    class SortedNumeric(Numeric):
        def __getitem__(self, id):
            self.docvalues.document = id
            return tuple(self.type(self.docvalues.valueAt(index)) for index in range(self.docvalues.count()))

    class SortedSet(Sorted):
        def __getitem__(self, id):
            self.docvalues.document = id
            ords = iter(self.docvalues.nextOrd, self.docvalues.NO_MORE_ORDS)
            return tuple(self.type(self.docvalues.lookupOrd(ord)) for ord in ords)


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
        if not isinstance(doc, string_types):
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
        builder = search.PhraseQuery.Builder()
        for position, term in zip(query.positions, query.terms):
            builder.add(self.correct(term), position)
        return builder.build()

    def getFieldQuery_quoted(self, *args):
        return self.rewrite(self.getFieldQuery_quoted_super(*args))

    def getFieldQuery_slop(self, *args):
        return self.rewrite(self.getFieldQuery_slop_super(*args))
