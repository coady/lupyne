"""
Convenient Query creation.

Operator overloading is used for combining boolean clauses.
"""

import lucene
from org.apache.lucene import index, search
from org.apache.lucene.search import spans
from lupyne.engine import Query
assert lucene.getVMEnv() or lucene.initVM()

# # # lucene # # #

q1 = search.TermQuery(index.Term('text', 'lucene'))
builder = search.PhraseQuery.Builder()
builder.add(index.Term('text', 'search'))
builder.add(index.Term('text', 'engine'))
q2 = builder.build()
builder = search.BooleanQuery.Builder()
builder.add(q1, search.BooleanClause.Occur.MUST)
builder.add(q2, search.BooleanClause.Occur.MUST)
q3 = builder.build()
assert str(q3) == '+text:lucene +text:"search engine"'

q1 = spans.SpanTermQuery(index.Term('text', 'hello'))
q2 = spans.SpanTermQuery(index.Term('text', 'world'))
q3 = spans.SpanPositionRangeQuery(q1, 0, 10)
q4 = spans.SpanNearQuery([q1, q2], 0, True)
q5 = spans.SpanNotQuery(q3, q4)
assert str(q5) == 'spanNot(spanPosRange(text:hello, 0, 10), spanNear([text:hello, text:world], 0, true), 0, 0)'

# # # lupyne # # #

q = Query.term('text', 'lucene') & Query.phrase('text', 'search', 'engine')
assert isinstance(q, search.BooleanQuery)
assert str(q) == '+text:lucene +text:"search engine"'

q = Query.span('text', 'hello')[:10] - Query.near('text', 'hello', 'world')
assert isinstance(q, spans.SpanQuery)
assert str(q) == 'spanNot(spanPosRange(text:hello, 0, 10), spanNear([text:hello, text:world], 0, true), 0, 0)'
