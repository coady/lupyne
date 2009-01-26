"""
Query wrappers and search utilities.
"""

import lucene

class Query(object):
    "Query wrapper which exploits operator overloading for easier creation."
    def __init__(self, q):
        self.q = q
    @classmethod
    def term(cls, name, value):
        "Create wrapped lucene TermQuery."
        return cls(lucene.TermQuery(lucene.Term(name, value)))
    @classmethod
    def prefix(cls, name, value):
        "Create wrapped lucene PrefixQuery."
        return cls(lucene.PrefixQuery(lucene.Term(name, value)))
    @classmethod
    def range(cls, name, lower, upper, inclusive=False):
        "Create wrapped lucene RangeQuery."
        return cls(lucene.RangeQuery(lucene.Term(name, lower), lucene.Term(name, upper), inclusive))
    def __and__(self, other):
        q = lucene.BooleanQuery()
        q.add(self.q, lucene.BooleanClause.Occur.MUST)
        q.add(other.q if isinstance(other, Query) else other, lucene.BooleanClause.Occur.MUST)
        return type(self)(q)
    def __or__(self, other):
        q = lucene.BooleanQuery()
        q.add(self.q, lucene.BooleanClause.Occur.SHOULD)
        q.add(other.q if isinstance(other, Query) else other, lucene.BooleanClause.Occur.SHOULD)
        return type(self)(q)
    def __sub__(self, other):
        q = lucene.BooleanQuery()
        q.add(self.q, lucene.BooleanClause.Occur.MUST)
        q.add(other.q if isinstance(other, Query) else other, lucene.BooleanClause.Occur.MUST_NOT)
        return type(self)(q)

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

class Filter(lucene.PythonFilter):
    "Filter a set of ids."
    def __init__(self, ids):
        lucene.PythonFilter.__init__(self)
        self.ids = ids
    def getDocIdSet(self, reader):
        bits = lucene.BitSet(reader.maxDoc())
        for id in self.ids:
            bits.set(id)
        return bits
    bits = getDocIdSet  # deprecated in Lucene, but still used in PyLucene
