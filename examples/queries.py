"""
Convenient Query creation.

Operator overloading is used for combining boolean clauses.
"""

import lucene
lucene.initVM()
from lupyne.engine import Query

### lucene ###

q1 = lucene.TermQuery(lucene.Term('text', 'lucene'))
q2 = lucene.PhraseQuery()
q2.add(lucene.Term('text', 'search'))
q2.add(lucene.Term('text', 'engine'))
q3 = lucene.BooleanQuery()
q3.add(q1, lucene.BooleanClause.Occur.MUST)
q3.add(q2, lucene.BooleanClause.Occur.MUST)
assert str(q3) == '+text:lucene +text:"search engine"'

q1 = lucene.SpanTermQuery(lucene.Term('text', 'hello'))
q2 = lucene.SpanFirstQuery(q1, 10)
q3 = lucene.SpanNotQuery(q1, q2)
assert str(q3) == 'spanNot(text:hello, spanFirst(text:hello, 10))'

### lupyne ###

q = Query.term('text', 'lucene') & Query.phrase('text', 'search', 'engine')
assert isinstance(q, lucene.BooleanQuery)
assert str(q) == '+text:lucene +text:"search engine"'

q = Query.span('text', 'hello')
q -= q[:10]
assert str(q) == 'spanNot(text:hello, spanFirst(text:hello, 10))'
