"""
Query usage.
"""

import lucene
lucene.initVM(lucene.CLASSPATH)
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

### lupyne ###

q = Query.term('text', 'lucene') & Query.phrase('text', 'search', 'engine')
assert isinstance(q, lucene.BooleanQuery)
assert str(q) == '+text:lucene +text:"search engine"'
