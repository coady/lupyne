"""
Query wrappers and search utilities.
"""

import itertools
import lucene

class Query(object):
    """Inherited lucene Query, with dynamic base class acquisition.
    Uses class methods and operator overloading for convenient query construction.
    """
    def __new__(cls, base, *args):
        return base.__new__(type(base.__name__, (cls, base), {}))
    def __init__(self, base, *args):
        base.__init__(self, *args)
    def filter(self, cache=True):
        "Return lucene CachingWrapperFilter, optionally just QueryWrapperFilter."
        filter = lucene.QueryWrapperFilter(self)
        return lucene.CachingWrapperFilter(filter) if cache else filter
    @classmethod
    def term(cls, name, value):
        "Return lucene TermQuery."
        return cls(lucene.TermQuery, lucene.Term(name, value))
    @classmethod
    def boolean(cls, occur, *queries, **terms):
        self = BooleanQuery(lucene.BooleanQuery)
        for query in queries:
            self.add(query, occur)
        for name, values in terms.items():
            for value in ([values] if isinstance(values, basestring) else values):
                self.add(cls.term(name, value), occur)
        return self
    @classmethod
    def any(cls, *queries, **terms):
        "Return lucene BooleanQuery (OR) from queries and terms."
        return cls.boolean(lucene.BooleanClause.Occur.SHOULD, *queries, **terms)
    @classmethod
    def all(cls, *queries, **terms):
        "Return lucene BooleanQuery (AND) from queries and terms."
        return cls.boolean(lucene.BooleanClause.Occur.MUST, *queries, **terms)
    @classmethod
    def span(cls, name, value):
        "Return lucene SpanTermQuery."
        return SpanQuery(lucene.SpanTermQuery, lucene.Term(name, value))
    @classmethod
    def prefix(cls, name, value):
        "Return lucene PrefixQuery."
        return cls(lucene.PrefixQuery, lucene.Term(name, value))
    @classmethod
    def range(cls, name, start, stop, lower=True, upper=False):
        "Return lucene RangeQuery, by default with a half-open interval."
        base = lucene.TermRangeQuery if hasattr(lucene, 'TermRangeQuery') else lucene.ConstantScoreRangeQuery
        return cls(base, name, start, stop, lower, upper)
    @classmethod
    def phrase(cls, name, *values):
        "Return lucene PhraseQuery.  None may be used as a placeholder."
        self = cls(lucene.PhraseQuery)
        for index, value in enumerate(values):
            if value is not None:
                self.add(lucene.Term(name, value), index)
        return self
    @classmethod
    def multiphrase(cls, name, *values):
        "Return lucene MultiPhraseQuery.  None may be used as a placeholder."
        self = cls(lucene.MultiPhraseQuery)
        for index, words in enumerate(values):
            if isinstance(words, basestring):
                words = [words]
            if words is not None:
                self.add([lucene.Term(name, word) for word in words], index)
        return self
    @classmethod
    def wildcard(cls, name, value):
        "Return lucene WildcardQuery."
        return cls(lucene.WildcardQuery, lucene.Term(name, value))
    @classmethod
    def fuzzy(cls, name, value, minimumSimilarity=0.5, prefixLength=0):
        "Return lucene FuzzyQuery."
        return cls(lucene.FuzzyQuery, lucene.Term(name, value), minimumSimilarity, prefixLength)
    def __pos__(self):
        return Query.all(self)
    def __neg__(self):
        return Query.boolean(lucene.BooleanClause.Occur.MUST_NOT, self)
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
    def __len__(self):
        return len(self.getClauses())
    def __iter__(self):
        return iter(self.getClauses())
    def __getitem__(self, index):
        return self.getClauses()[index]
    def __iand__(self, other):
        self.add(other, lucene.BooleanClause.Occur.MUST)
        return self
    def __ior__(self, other):
        self.add(other, lucene.BooleanClause.Occur.SHOULD)
        return self
    def __isub__(self, other):
        self.add(other, lucene.BooleanClause.Occur.MUST_NOT)
        return self

class SpanQuery(Query):
    def __getitem__(self, slc):
        assert slc.start is slc.step is None, 'only prefix slice supported'
        return SpanQuery(lucene.SpanFirstQuery, self, slc.stop)
    def __sub__(self, other):
        return SpanQuery(lucene.SpanNotQuery, self, other)
    def __or__(*spans):
        return SpanQuery(lucene.SpanOrQuery, spans)
    def near(*spans, **kwargs):
        """Return lucene SpanNearQuery.
        
        :param slop: default 0
        :param inOrder: default True
        """
        slop = kwargs.pop('slop', 0)
        inOrder = kwargs.pop('inOrder', True)
        return SpanQuery(lucene.SpanNearQuery, spans, slop, inOrder, **kwargs)

class HitCollector(lucene.PythonCollector if hasattr(lucene, 'PythonCollector') else lucene.PythonHitCollector):
    "Collect all ids and scores efficiently."
    def __init__(self):
        super(HitCollector, self).__init__()
        self.base = 0
        self.scores = {}
    def collect(self, id, score):
        self.scores[id + self.base] = score
    def setNextReader(self, reader, base):
        self.base = base
    def acceptsDocsOutOfOrder(self):
        return True
    def sorted(self, key=None, reverse=False):
        "Return ordered ids and scores."
        ids = sorted(self.scores)
        if key is None:
            key, reverse = self.scores.__getitem__, True
        ids.sort(key=key, reverse=reverse)
        return ids, map(self.scores.__getitem__, ids)

class Filter(lucene.PythonFilter):
    "Inherited lucene Filter with a cached BitSet of ids."
    def __init__(self, ids):
        lucene.PythonFilter.__init__(self)
        self.docIdSet = lucene.OpenBitSet()
        if isinstance(ids, lucene.OpenBitSet):
            self.docIdSet.union(ids)
        else:
            setter = self.docIdSet.set
            for id in itertools.imap(long, ids):
                setter(id)
        if lucene.VERSION < '3':
            self.bitSet = lucene.BitSet()
            setter = self.bitSet.set
            for id in itertools.ifilter(self.docIdSet.get, xrange(self.docIdSet.size())):
                setter(id)
    def overlap(self, other, reader=None):
        "Return intersection count of the filters."
        return int(lucene.OpenBitSet.intersectionCount(self.getDocIdSet(reader), other.getDocIdSet(reader)))
    def bits(self, reader=None):
        "Return cached BitSet, reader is ignored.  Deprecated."
        return self.bitSet
    def getDocIdSet(self, reader=None):
        "Return cached OpenBitSet, reader is ignored."
        return self.docIdSet

class Highlighter(lucene.Highlighter):
    """Inherited lucene Filter with stored analysis options.
    Using span scoring in lucene 2.4 is not thread-safe.
    
    :param query: lucene Query
    :param analyzer: analyzer for texts
    :param span: only highlight terms which would contribute to a hit
    :param formatter: optional lucene Formatter or html tag name
    :param encoder: optional lucene Encoder
    :param field: optional field name used to match query terms
    :param reader: optional lucene IndexReader to compute term weights
    """
    def __init__(self, query, analyzer, span=True, formatter=None, encoder=None, field=None, reader=None):
        if isinstance(formatter, basestring):
            formatter = lucene.SimpleHTMLFormatter('<{0}>'.format(formatter), '</{0}>'.format(formatter))
        scorer = lucene.QueryScorer if (span or not hasattr(lucene, 'QueryTermScorer')) else lucene.QueryTermScorer
        scorer = scorer(*filter(None, [query, reader, field]))
        lucene.Highlighter.__init__(self, *filter(None, [formatter, encoder, scorer]))
        self.query, self.analyzer, self.span, self.field = query, analyzer, span, field
    def fragments(self, text, count=1, field=None):
        """Return highlighted text fragments.
        
        :param text: text string to be searched
        :param count: maximum number of fragments
        :param field: optional field to use for text analysis
        """
        if lucene.VERSION <= '2.4.1': # memory leak in string array
            tokens = self.analyzer.tokenStream(field, lucene.StringReader(text))
            if self.span:
                tokens = lucene.CachingTokenFilter(tokens)
                self.fragmentScorer = lucene.HighlighterSpanScorer(self.query, self.field, tokens)
            return map(unicode, self.getBestTextFragments(tokens, text, True, count))
        return list(self.getBestFragments(self.analyzer, field, text, count))
