"""
Query wrappers and search utilities.
"""

import itertools
import lucene

class Query(object):
    """Delegated lucene Query.
    Uses class methods and operator overloading for convenient query construction.
    """
    def __init__(self, q):
        self.q = q
    def __getattr__(self, name):
        if name == 'q':
            raise AttributeError(name)
        return getattr(self.q, name)
    def __str__(self):
        return str(self.q)
    def filter(self, cache=True):
        "Return lucene CachingWrapperFilter, optionally just QueryWrapperFilter."
        filter = lucene.QueryWrapperFilter(self.q)
        return lucene.CachingWrapperFilter(filter) if cache else filter
    @classmethod
    def term(cls, name, value):
        "Create wrapped lucene TermQuery."
        return cls(lucene.TermQuery(lucene.Term(name, value)))
    @classmethod
    def boolean(cls, occur, *queries, **terms):
        q = lucene.BooleanQuery()
        for query in queries + tuple(map(cls.term, terms, terms.values())):
            q.add(query.q if isinstance(query, Query) else query, occur)
        return cls(q)
    @classmethod
    def any(cls, *queries, **terms):
        "Return boolean OR query from queries and terms."
        return cls.boolean(lucene.BooleanClause.Occur.SHOULD, *queries, **terms)
    @classmethod
    def all(cls, *queries, **terms):
        "Return boolean AND query from queries and terms."
        return cls.boolean(lucene.BooleanClause.Occur.MUST, *queries, **terms)
    @classmethod
    def prefix(cls, name, value):
        "Create wrapped lucene PrefixQuery."
        return cls(lucene.PrefixQuery(lucene.Term(name, value)))
    @classmethod
    def range(cls, name, start, stop):
        "Create wrapped half-open lucene ConstantScoreRangeQuery."
        return cls(lucene.ConstantScoreRangeQuery(name, start, stop, True, False))
    @classmethod
    def phrase(cls, name, *values):
        "Create wrapped lucene PhraseQuery.  None may be used as a placeholder."
        q = lucene.PhraseQuery()
        for index, value in enumerate(values):
            if value is not None:
                q.add(lucene.Term(name, value), index)
        return cls(q)
    def __and__(self, other):
        return self.all(self, other)
    def __or__(self, other):
        return self.any(self, other)
    def __sub__(self, other):
        query = self.all(self)
        query.q.add(other.q if isinstance(other, Query) else other, lucene.BooleanClause.Occur.MUST_NOT)
        return query

class HitCollector(lucene.PythonHitCollector):
    "Collect all ids and scores efficiently."
    def __init__(self, searcher):
        lucene.PythonHitCollector.__init__(self, searcher)
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
