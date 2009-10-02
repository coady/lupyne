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
        "Return lucene ConstantScoreRangeQuery, by default with a half-open interval."
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
    def __or__(self, other):
        return Query.any(self, other)
    def __sub__(self, other):
        return Query.any(self).__isub__(other)

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

class HitCollector(lucene.PythonHitCollector):
    "Collect all ids and scores efficiently."
    def __init__(self):
        lucene.PythonHitCollector.__init__(self)
        self.collect = {}.__setitem__
    def sorted(self, key=None, reverse=False):
        "Return ordered ids and scores."
        data = self.collect.__self__
        ids = sorted(data)
        if key is None:
            key, reverse = data.__getitem__, True
        ids.sort(key=key, reverse=reverse)
        return ids, map(data.__getitem__, ids)

class BitSet(lucene.BitSet):
    "Inherited lucene BitSet with a set interface."
    __len__ = lucene.BitSet.cardinality
    __contains__ = lucene.BitSet.get
    add = lucene.BitSet.set
    def __init__(self, ids=()):
        lucene.BitSet.__init__(self)
        if isinstance(ids, lucene.BitSet):
            self |= ids
        else:
            for id in ids:
                self.set(id)
    def discard(self, id):
        self.set(id, False)
    def __iter__(self):
        return itertools.ifilter(self.get, xrange(self.length()))
    def __ior__(self, other):
        getattr(self, 'or')(other)
        return self
    def __iand__(self, other):
        getattr(self, 'and')(other)
        return self
    def __isub__(self, other):
        self.andNot(other)
        return self
    def __or__(self, other):
        return type(self)(self).__ior__(other)
    def __and__(self, other):
        return type(self)(self).__iand__(other)
    def __sub__(self, other):
        return type(self)(self).__isub__(other)

class Filter(lucene.PythonFilter):
    "Inherited lucene Filter with a cached BitSet of ids."
    def __init__(self, ids):
        lucene.PythonFilter.__init__(self)
        self._bits = BitSet(ids)
    def bits(self, reader=None):
        """Return cached BitSet.
        Although this method is deprecated in Lucene, it's in use in PyLucene.
        
        :param reader: ignored IndexReader, necessary for lucene api
        """
        return self._bits
