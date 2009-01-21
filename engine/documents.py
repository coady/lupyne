"""
Document wrappers
"""

import itertools
import lucene

class Field(object):
    "Saved parameters which can generate lucene Fields given values."
    Parameters = lucene.Field.Store, lucene.Field.Index, lucene.Field.TermVector
    def __init__(self, name, store=False, index='analyzed', termvector=False):
        "Assign field parameters from text or boolean descriptors, with lucene defaults."
        self.name = name
        if isinstance(store, bool):
            store = 'yes' if store else 'no'
        if isinstance(index, bool):
            index = 'not_analyzed' if index else 'no'
        if isinstance(termvector, bool):
            termvector = 'yes' if termvector else 'no'
        self.params = [getattr(name, value.upper()) for name, value in zip(self.Parameters, [store, index, termvector])]
    def items(self, *values):
        "Generate lucene Fields suitable for adding to a document."
        for value in values:
            yield lucene.Field(self.name, value, *self.params)

class Document(object):
    """Delegated lucene Document.
    
    Supports mapping interface of field names to values, but duplicate field names are allowed."""
    Fields = lucene.Field.Store, lucene.Field.Index, lucene.Field.TermVector
    def __init__(self, doc=None):
        self.doc = lucene.Document() if doc is None else doc
    def add(self, name, value, **params):
        "Add field to document with given parameters."
        for field in Field(name, **params).items(value):
            self.doc.add(field)
    def fields(self):
        return itertools.imap(lucene.Field.cast_, self.doc.getFields())
    def __iter__(self):
        for field in self.fields():
            yield field.name()
    def items(self):
        for field in self.fields():
            yield field.name(), field.stringValue()
    def __getitem__(self, name):
        value = self.doc[name]
        if value is None:
            raise KeyError(value)
        return value
    def get(self, name, default=None):
        value = self.doc[name]
        return default if value is None else value
    def __delitem__(self, name):
        self.doc.removeFields(name)
    def getlist(self, name):
        "Return multiple field values."
        return list(self.doc.getValues(name))

class Hit(Document):
    "A Document with an id and score, from a search result."
    def __init__(self, doc, id, score):
        Document.__init__(self, doc)
        self.id, self.score = id, score
    def items(self):
        "Include id and score using python naming convention."
        fields = {'__id__': self.id, '__score__': self.score}
        return itertools.chain(fields.items(), Document.items(self))

class Hits(object):
    """Search results: lazily evaluated and memory efficient.
    
    Supports a read-only sequence interface to hit objects.
    @searcher: IndexSearcher which can retrieve documents.
    @ids: ordered doc ids.
    @scores: ordered doc scores.
    @count: total number of hits."""
    def __init__(self, searcher, ids, scores, count=0):
        self.searcher = searcher
        self.ids, self.scores = ids, scores
        self.count = count or len(self)
    def __len__(self):
        return len(self.ids)
    def __getitem__(self, index):
        id, score = self.ids[index], self.scores[index]
        if isinstance(index, slice):
            return type(self)(self.searcher, id, score, self.count)
        return Hit(self.searcher.doc(id), id, score)
    def items(self):
        "Generate zipped ids and scores."
        return itertools.izip(self.ids, self.scores)
