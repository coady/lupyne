from typing import Callable, Iterable, Iterator
import lucene  # noqa
from java.lang import Double, Integer, Long
from java.util import Arrays
from org.apache.lucene import document, index, search, util
from org.apache.lucene.queries import spans
from org.apache.pylucene.queryparser.classic import PythonQueryParser


class Query:
    """Inherited lucene Query, with dynamic base class acquisition.

    Uses class methods and operator overloading for convenient query construction.
    """

    def __new__(cls, base, *args):
        return base.__new__(type(base.__name__, (cls, base), {}))

    def __init__(self, base: search.Query, *args):
        base.__init__(self, *args)

    @classmethod
    def term(cls, name: str, value) -> 'Query':
        """Return lucene TermQuery."""
        return cls(search.TermQuery, index.Term(name, value))

    @classmethod
    def terms(cls, name: str, values) -> 'Query':
        """Return lucene TermInSetQuery, optimizing a SHOULD BooleanQuery of many terms."""
        return cls(search.TermInSetQuery, name, list(map(util.BytesRef, values)))

    @classmethod
    def boolean(cls, occur, *queries, **terms):
        builder = search.BooleanQuery.Builder()
        for query in queries:
            builder.add(query, occur)
        for name, values in terms.items():
            for value in [values] if isinstance(values, str) else values:
                builder.add(cls.term(name, value), occur)
        return builder.build()

    @classmethod
    def any(cls, *queries: search.Query, **terms) -> search.BooleanQuery:
        """Return lucene BooleanQuery with SHOULD clauses from queries and terms."""
        return cls.boolean(search.BooleanClause.Occur.SHOULD, *queries, **terms)

    @classmethod
    def all(cls, *queries: search.Query, **terms) -> search.BooleanQuery:
        """Return lucene BooleanQuery with MUST clauses from queries and terms."""
        return cls.boolean(search.BooleanClause.Occur.MUST, *queries, **terms)

    @classmethod
    def filter(cls, *queries: search.Query, **terms) -> search.BooleanQuery:
        """Return lucene BooleanQuery with FILTER clauses from queries and terms."""
        return cls.boolean(search.BooleanClause.Occur.FILTER, *queries, **terms)

    @classmethod
    def disjunct(cls, multiplier, *queries, **terms):
        """Return lucene DisjunctionMaxQuery from queries and terms."""
        queries = list(queries)
        for name, values in terms.items():
            queries += (cls.term(name, value) for value in ([values] if isinstance(values, str) else values))
        return cls(search.DisjunctionMaxQuery, Arrays.asList(queries), multiplier)

    @classmethod
    def span(cls, *term) -> 'SpanQuery':
        """Return [SpanQuery][lupyne.engine.queries.SpanQuery] from term name and value or a MultiTermQuery."""
        if len(term) <= 1:
            return SpanQuery(spans.SpanMultiTermQueryWrapper, *term)
        return SpanQuery(spans.SpanTermQuery, index.Term(*term))

    @classmethod
    def near(cls, name: str, *values, **kwargs) -> 'SpanQuery':
        """Return [SpanNearQuery][lupyne.engine.queries.SpanQuery.near] from terms.
        Term values which supply another field name will be masked."""
        spans = (cls.span(name, value) if isinstance(value, str) else cls.span(*value).mask(name) for value in values)
        return SpanQuery.near(*spans, **kwargs)

    @classmethod
    def prefix(cls, name: str, value) -> 'Query':
        """Return lucene PrefixQuery."""
        return cls(search.PrefixQuery, index.Term(name, value))

    @classmethod
    def range(cls, name: str, start, stop, lower=True, upper=False) -> 'Query':
        """Return lucene RangeQuery, by default with a half-open interval."""
        start, stop = (value if value is None else util.BytesRef(value) for value in (start, stop))
        return cls(search.TermRangeQuery, name, start, stop, lower, upper)

    @classmethod
    def phrase(cls, name: str, *values, **attrs) -> search.MultiPhraseQuery:
        """Return lucene MultiPhraseQuery.  None may be used as a placeholder."""
        builder = search.MultiPhraseQuery.Builder()
        for attr in attrs:
            setattr(builder, attr, attrs[attr])
        for idx, words in enumerate(values):
            if isinstance(words, str):
                words = [words]
            if words is not None:
                builder.add([index.Term(name, word) for word in words], idx)
        return builder.build()

    @classmethod
    def wildcard(cls, name: str, value) -> 'Query':
        """Return lucene WildcardQuery."""
        return cls(search.WildcardQuery, index.Term(name, value))

    @classmethod
    def fuzzy(cls, name: str, value, *args) -> 'Query':
        """Return lucene FuzzyQuery."""
        return cls(search.FuzzyQuery, index.Term(name, value), *args)

    @classmethod
    def alldocs(cls) -> 'Query':
        """Return lucene MatchAllDocsQuery."""
        return cls(search.MatchAllDocsQuery)

    @classmethod
    def nodocs(cls) -> 'Query':
        """Return lucene MatchNoDocsQuery."""
        return cls(search.MatchNoDocsQuery)

    @classmethod
    def regexp(cls, name: str, value, *args) -> 'Query':
        """Return lucene RegexpQuery."""
        return cls(search.RegexpQuery, index.Term(name, value), *args)

    @staticmethod
    def points(name: str, *values) -> search.Query:
        """Return lucene set query of one dimensional points."""
        if any(isinstance(value, float) for value in values):
            return document.DoublePoint.newSetQuery(name, values)
        return document.LongPoint.newSetQuery(name, tuple(map(int, values)))

    @staticmethod
    def ranges(name: str, *intervals, lower=True, upper=False) -> search.Query:
        """Return lucene multidimensional point range query, by default with half-open intervals."""
        starts, stops = [], []
        for start, stop in intervals:
            if isinstance(start, float) or isinstance(stop, float):
                if start is None:
                    start = Double.NEGATIVE_INFINITY
                elif not lower:
                    start = document.DoublePoint.nextUp(start)
                if stop is None:
                    stop = Double.POSITIVE_INFINITY
                elif not upper:
                    stop = document.DoublePoint.nextDown(stop)
            else:
                if start is None:
                    start = Long.MIN_VALUE
                elif not lower:
                    start += 1
                if stop is None:
                    stop = Long.MAX_VALUE
                elif not upper:
                    stop -= 1
                start, stop = int(start), int(stop)
            starts.append(start)
            stops.append(stop)
        if any(isinstance(value, float) for value in starts):
            return document.DoublePoint.newRangeQuery(name, starts, stops)
        return document.LongPoint.newRangeQuery(name, starts, stops)

    def constant(self) -> 'Query':
        """Return lucene ConstantScoreQuery."""
        return Query(search.ConstantScoreQuery, self)

    def boost(self, value: float) -> 'Query':
        """Return lucene BoostQuery."""
        return Query(search.BoostQuery, self, value)

    def __pos__(self) -> search.BooleanQuery:
        """+self"""
        return Query.all(self)

    def __neg__(self) -> search.BooleanQuery:
        """-self"""
        return Query.boolean(search.BooleanClause.Occur.MUST_NOT, self)

    def __and__(self, other: search.Query) -> search.BooleanQuery:
        """+self +other"""
        return Query.all(self, other)

    def __rand__(self, other):
        return Query.all(other, self)

    def __or__(self, other: search.Query) -> search.BooleanQuery:
        """self other"""
        return Query.any(self, other)

    def __ror__(self, other):
        return Query.any(other, self)

    def __sub__(self, other: search.Query) -> search.BooleanQuery:
        """self -other"""
        builder = search.BooleanQuery.Builder()
        builder.add(self, search.BooleanClause.Occur.SHOULD)
        builder.add(other, search.BooleanClause.Occur.MUST_NOT)
        return builder.build()

    def __rsub__(self, other):
        return Query.__sub__(other, self)


class SpanQuery(Query):
    """Inherited lucene SpanQuery with additional span constructors."""

    def __getitem__(self, slc: slice) -> 'SpanQuery':
        start, stop, step = slc.indices(Integer.MAX_VALUE)
        assert step == 1, 'slice step is not supported'
        return SpanQuery(spans.SpanPositionRangeQuery, self, start, stop)

    def __sub__(self, other: spans.SpanQuery) -> 'SpanQuery':
        return SpanQuery(spans.SpanNotQuery, self, other)

    def __or__(*spans_: spans.SpanQuery) -> 'SpanQuery':
        return SpanQuery(spans.SpanOrQuery, spans_)

    def near(*spans_, slop=0, inOrder=True):
        """Return lucene SpanNearQuery from SpanQueries."""
        return SpanQuery(spans.SpanNearQuery, spans_, slop, inOrder)

    def mask(self, name: str) -> 'SpanQuery':
        """Return lucene FieldMaskingSpanQuery, which allows combining span queries from different fields."""
        return SpanQuery(spans.FieldMaskingSpanQuery, self, name)

    def containing(self, other: spans.SpanQuery) -> 'SpanQuery':
        """Return lucene SpanContainingQuery."""
        return SpanQuery(spans.SpanContainingQuery, self, other)

    def within(self, other: spans.SpanQuery) -> 'SpanQuery':
        """Return lucene SpanWithinQuery."""
        return SpanQuery(spans.SpanWithinQuery, self, other)


class DocValues:
    """DocValues with type conversion."""

    class Sorted:
        def __init__(self, docvalues: index.DocValues, size: int, type: Callable):
            self.docvalues, self.size, self.type = docvalues, size, type

        def __iter__(self):
            return map(self.__getitem__, range(self.size))

        def select(self, ids: Iterable) -> dict:
            """Return mapping of doc ids to values."""
            return {id: self[id] for id in sorted(ids)}

        def __getitem__(self, id: int):
            if self.docvalues.advanceExact(id):  # pragma: no branch
                return self.type(self.docvalues.lookupOrd(self.docvalues.ordValue()))

    class Binary(Sorted):
        def __getitem__(self, id: int):
            if self.docvalues.advanceExact(id):
                return self.type(self.docvalues.binaryValue())

    class Numeric(Sorted):
        def __getitem__(self, id: int):
            if self.docvalues.advanceExact(id):
                return self.type(self.docvalues.longValue())

    class SortedNumeric(Sorted):
        def __getitem__(self, id: int):
            if self.docvalues.advanceExact(id):
                return tuple(self.type(self.docvalues.nextValue()) for _ in range(self.docvalues.docValueCount()))

    class SortedSet(Sorted):
        def __getitem__(self, id: int):
            ords: Iterator = iter(self.docvalues.nextOrd, self.docvalues.NO_MORE_ORDS)
            if self.docvalues.advanceExact(id):
                return tuple(self.type(self.docvalues.lookupOrd(ord)) for ord in ords)


class SpellParser(PythonQueryParser):
    """Inherited lucene QueryParser which corrects spelling.

    Assign a searcher attribute or override [suggest][lupyne.engine.queries.SpellParser.suggest] implementation.
    """

    def suggest(self, term: index.Term) -> index.Term:
        """Return term with text replaced as necessary."""
        field = term.field()
        words = self.searcher.suggest(field, term.text())
        return index.Term(field, *words) if words else term

    def rewrite(self, query: search.Query) -> search.Query:
        """Return term or phrase query with corrected terms substituted."""
        if search.TermQuery.instance_(query):
            term = search.TermQuery.cast_(query).term
            return search.TermQuery(self.suggest(term))
        query = search.PhraseQuery.cast_(query)
        builder = search.PhraseQuery.Builder()
        for position, term in zip(query.positions, query.terms):
            builder.add(self.suggest(term), position)
        return builder.build()

    def getFieldQuery_quoted(self, *args):
        return self.rewrite(self.getFieldQuery_quoted_super(*args))

    def getFieldQuery_slop(self, *args):
        return self.rewrite(self.getFieldQuery_slop_super(*args))
