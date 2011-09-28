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
q2 = lucene.SpanTermQuery(lucene.Term('text', 'world'))
q3 = lucene.SpanPositionRangeQuery(q1, 0, 10)
q4 = lucene.SpanNearQuery([q1, q2], 0, True)
q5 = lucene.SpanNotQuery(q3, q4)
assert str(q5) == 'spanNot(spanPosRange(text:hello, 0, 10), spanNear([text:hello, text:world], 0, true))'

### lupyne ###

q = Query.term('text', 'lucene') & Query.phrase('text', 'search', 'engine')
assert isinstance(q, lucene.BooleanQuery)
assert str(q) == '+text:lucene +text:"search engine"'

q = Query.span('text', 'hello')[:10] - Query.near('text', 'hello', 'world')
assert isinstance(q, lucene.SpanQuery)
assert str(q) == 'spanNot(spanPosRange(text:hello, 0, 10), spanNear([text:hello, text:world], 0, true))'
