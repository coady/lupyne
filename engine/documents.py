"""
Wrappers for lucene Fields and Documents.
"""

import itertools
import lucene

class Field(list):
    """Saved parameters which can generate lucene Fields given values.
    
    :param name: name of field
    :param store, index, termvector: field parameters, expressed as bools or strs, with lucene defaults.
    """
    Names = 'Store', 'Index', 'TermVector'
    Parameters = tuple(getattr(lucene.Field, name) for name in Names)
    def __init__(self, name, store=False, index='analyzed', termvector=False):
        if isinstance(store, bool):
            store = 'yes' if store else 'no'
        if isinstance(index, bool):
            index = 'not_analyzed' if index else 'no'
        if isinstance(termvector, bool):
            termvector = 'yes' if termvector else 'no'
        items = zip(self.Parameters, [store, index, termvector])
        self += (getattr(param, value.upper()) for param, value in items)
        self.name = name
        for index, name in enumerate(self.Names):
            setattr(self, name.lower(), str(self[index]))
    def items(self, *values):
        "Generate lucene Fields suitable for adding to a document."
        for value in values:
            yield lucene.Field(self.name, value, *self)

class NestedField(Field):
    """Field which indexes every component into its own field.
    The original value may be optionally stored, but components will be only indexed.
    
    :param sep: field separator.
    """
    def __init__(self, name, store=False, index=False, termvector=False, sep=':'):
        Field.__init__(self, name, store, index, termvector)
        self.sep = sep
        # these params are instance variables because they don't exist before initVM is called
        self.params = lucene.Field.Store.NO, lucene.Field.Index.NOT_ANALYZED
    def getname(self, words):
        "Return customized name for given words."
        return self.sep.join([self.name] + words)
    def items(self, *values):
        "Generate original field (if necessary) and all component fields."
        for value in values:
            try:
                yield lucene.Field(self.name, value, *self)
            except lucene.JavaError:
                pass # field might not be stored or indexed
            words = value.split(self.sep)
            for index, word in enumerate(words):
                name = self.getname(words[:index])
                yield lucene.Field(name, word, *self.params)
    def query(self, value):
        "Return lucene TermQuery of the appropriate field depth."
        words = value.split(self.sep)
        value = words.pop()
        return lucene.TermQuery(lucene.Term(self.getname(words), value))

class PrefixField(NestedField):
    """Field indexed with a prefix tree.
    Unlike a normal nested field, the field names only refer to the depth, while the values are nested.
    The component field names are expressed as slices of the original values, but may be customized.
    
    :param start, stop, step: slice parameters, which respect the python convention of half-open intervals.
    """
    def __init__(self, name, store=False, index=True, termvector=False, sep='', start=1, stop=None, step=1):
        NestedField.__init__(self, name, store, index, termvector, sep=sep)
        self.slice = slice(start, stop, step)
    def split(self, value):
        return value.split(self.sep) if self.sep else value
    def join(self, words):
        return self.sep.join(words)
    def getname(self, index):
        "Return customized name for prefix field of given depth."
        return '%s[:%i]' % (self.name, index)
    def items(self, *values):
        """Generate tiered indexed fields along with the original value.
        Optimized to handle duplicate values.
        """
        for field in Field.items(self, *values):
            yield field
        values = [tuple(self.split(value)) for value in values]
        for index in range(*self.slice.indices(max(map(len, values)))):
            name = self.getname(index)
            for value in sorted(set(value[:index] for value in values if len(value) >= index)):
                yield lucene.Field(name, self.join(value), *self.params)
    def query(self, prefix):
        "Return lucene TermQuery of the appropriate prefixed field."
        name = self.getname(len(self.split(prefix)))
        return lucene.TermQuery(lucene.Term(name, prefix))

class Document(object):
    """Delegated lucene Document.
    Provides mapping interface of field names to values, but duplicate field names are allowed.
    
    :param doc: optional lucene Document.
    """
    Fields = lucene.Field.Store, lucene.Field.Index, lucene.Field.TermVector
    def __init__(self, doc=None):
        self.doc = lucene.Document() if doc is None else doc
    def add(self, name, value, **params):
        "Add field to document with given parameters."
        for field in Field(name, **params).items(value):
            self.doc.add(field)
    def fields(self):
        "Generate lucene Fields."
        return itertools.imap(lucene.Field.cast_, self.doc.fields())
    def __len__(self):
        return self.doc.getFields().size()
    def __contains__(self, name):
        return self.doc[name] is not None
    def __iter__(self):
        for field in self.fields():
            yield field.name()
    def items(self):
        "Generate name, value pairs for all fields."
        for field in self.fields():
            yield field.name(), field.stringValue()
    def __getitem__(self, name):
        value = self.doc[name]
        if value is None:
            raise KeyError(name)
        return value
    def get(self, name, default=None):
        "Return field value if present, else default."
        value = self.doc[name]
        return default if value is None else value
    def __delitem__(self, name):
        self.doc.removeFields(name)
    def getlist(self, name):
        "Return list of all values for given field."
        return list(self.doc.getValues(name))
    def dict(self, *names, **defaults):
        """Return dict representation of document.
        
        :param names: names of multi-valued fields to return as a list.
        :param defaults: return only given fields, using default values as necessary."""
        for name, value in defaults.items():
            defaults[name] = self.get(name, value)
        if not defaults:
            defaults = dict(self.items())
        defaults.update(zip(names, map(self.getlist, names)))
        return defaults

class Hit(Document):
    "A Document with an id and score, from a search result."
    def __init__(self, doc, id, score):
        Document.__init__(self, doc)
        self.id, self.score = id, score
    def dict(self, *names, **defaults):
        "Return dict representation of document with __id__ and __score__."
        result = Document.dict(self, *names, **defaults)
        result.update(__id__=self.id, __score__=self.score)
        return result

class Hits(object):
    """Search results: lazily evaluated and memory efficient.
    Provides a read-only sequence interface to hit objects.
    
    :param searcher: `IndexSearcher`_ which can retrieve documents.
    :param ids: ordered doc ids.
    :param scores: ordered doc scores.
    :param count: total number of hits."""
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
