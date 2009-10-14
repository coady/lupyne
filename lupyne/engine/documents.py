"""
Wrappers for lucene Fields and Documents.
"""

import itertools
import datetime
import lucene
from .queries import Query

class Field(object):
    """Saved parameters which can generate lucene Fields given values.
    
    :param name: name of field
    :param store, index, termvector: field parameters, expressed as bools or strs, with lucene defaults
    :param attrs: additional attributes to set on the field
    """
    def __init__(self, name, store=False, index='analyzed', termvector=False, **attrs):
        self.name = name
        self.attrs = attrs
        if isinstance(store, bool):
            store = 'yes' if store else 'no'
        if isinstance(index, bool):
            index = 'not_analyzed' if index else 'no'
        if isinstance(termvector, bool):
            termvector = 'yes' if termvector else 'no'
        for name, value in zip(['Store', 'Index', 'TermVector'], [store, index, termvector]):
            setattr(self, name.lower(), getattr(getattr(lucene.Field, name), value.upper()))
    def items(self, *values):
        "Generate lucene Fields suitable for adding to a document."
        for value in values:
            try:
                field = lucene.Field(self.name, value, self.store, self.index, self.termvector)
            except lucene.InvalidArgsError:
                field = lucene.Field(self.name, value, self.termvector)
            for name, value in self.attrs.items():
                setattr(field, name, value)
            yield field

class FormatField(Field):
    """Field which uses string formatting on its values.
    
    :param format: format string
    """
    def __init__(self, name, format='{0}', **kwargs):
        Field.__init__(self, name, **kwargs)
        self.format = format.format
    def items(self, *values):
        "Generate fields with formatted values."
        return Field.items(self, *map(self.format, values))

class NumericField(Field):
    """Field which indexes numbers in a prefix tree.
    
    :param name: name of field
    :param step: precision step
    """
    def __init__(self, name, step=None, store=False, index=True):
        Field.__init__(self, name, store)
        self.step = step or lucene.NumericUtils.PRECISION_STEP_DEFAULT
        self.index = index
    def items(self, *values):
        "Generate lucene NumericFields suitable for adding to a document."
        for value in values:
            field = lucene.NumericField(self.name, self.step, self.store, self.index)
            if isinstance(value, float):
                field.doubleValue = value
            else:
                field.longValue = long(value)
            yield field
    def range(self, start, stop, lower=True, upper=False):
        "Return lucene NumericRangeQuery."
        if isinstance(start, float) or isinstance(stop, float):
            start, stop = (value if value is None else lucene.Double(value) for value in (start, stop))
            return lucene.NumericRangeQuery.newDoubleRange(self.name, start, stop, lower, upper)
        if start is not None:
            start = None if start < lucene.Long.MIN_VALUE else lucene.Long(long(start))
        if stop is not None:
            stop = None if start > lucene.Long.MAX_VALUE else lucene.Long(long(stop))
        return lucene.NumericRangeQuery.newLongRange(self.name, start, stop, lower, upper)

class PrefixField(Field):
    """Field which indexes every prefix of a value into a separate component field.
    The customizable component field names are expressed as slices.
    Original value may be stored for convenience.
    
    :param start, stop, step: optional slice parameters of the prefix depths (not indices)
    """
    def __init__(self, name, start=1, stop=None, step=1, store=False, index=True, termvector=False):
        Field.__init__(self, name, store, index, termvector)
        self.depths = slice(start, stop, step)
    def split(self, text):
        "Return immutable sequence of words from name or value."
        return text
    def join(self, words):
        "Return text from separate words."
        return words
    def getname(self, depth):
        "Return prefix field name for given depth."
        return '{0}[:{1:n}]'.format(self.name, depth)
    def indices(self, depth):
        "Return range of valid depth indices."
        return xrange(*self.depths.indices(depth + self.depths.step))
    def items(self, *values):
        """Generate indexed component fields.
        Optimized to handle duplicate values.
        """
        if self.store != lucene.Field.Store.NO:
            for value in values:
                yield lucene.Field(self.name, value, self.store, lucene.Field.Index.NO)
        values = map(self.split, values)
        for depth in self.indices(max(map(len, values))):
            name = self.getname(depth)
            for value in sorted(set(value[:depth] for value in values if len(value) > (depth-self.depths.step))):
                yield lucene.Field(name, self.join(value), lucene.Field.Store.NO, self.index, self.termvector)
    def prefix(self, value):
        "Return prefix query of the closest possible prefixed field."
        depths = self.indices(len(self.split(value)))
        depth = depths[-1] if depths else self.depths.start
        return Query.prefix(self.getname(depth), value)
    def range(self, start, stop, lower=True, upper=False):
        "Return range query of the closest possible prefixed field."
        depths = self.indices(max(len(self.split(value)) for value in (start, stop)))
        depth = depths[-1] if depths else self.depths.start
        return Query.range(self.getname(depth), start, stop, lower, upper)

class NestedField(PrefixField):
    """Field which indexes every component into its own field.
    
    :param sep: field separator used on name and values
    """
    def __init__(self, name, sep=':', **kwargs):
        PrefixField.__init__(self, name, **kwargs)
        self.sep = sep
        names = self.split(name)
        self.names = [self.join(names[:depth]) for depth in range(len(names)+1)]
    def split(self, text):
        "Return immutable sequence of words from name or value."
        return tuple(text.split(self.sep))
    def join(self, words):
        "Return text from separate words."
        return self.sep.join(words)
    def getname(self, depth):
        "Return component field name for given depth."
        return self.names[depth]

class DateTimeField(PrefixField):
    """Field which indexes each datetime component in sortable ISO format: Y-m-d H:M:S.
    Works with datetimes, dates, and any object whose string form is a prefix of ISO.
    """
    def split(self, text):
        "Return immutable sequence of datetime components."
        words = (words.split(char) for words, char in zip(text.split(), '-:'))
        return tuple(itertools.chain(*words))
    def join(self, words):
        "Return datetime components in ISO format."
        return ' '.join(filter(None, ['-'.join(words[:3]), ':'.join(words[3:])]))
    def getname(self, depth):
        "Return component field name for given depth."
        return '{0}:{1}'.format(self.name, 'YmdHMS'[:depth])
    def items(self, *dates):
        return PrefixField.items(self, *map(str, dates))
    def prefix(self, date):
        "Return prefix query of the datetime."
        return PrefixField.prefix(self, str(date))
    def _range(self, start, stop):
        depth = max(map(len, (start, stop))) - 1
        lower, upper = start[:depth], stop[:depth]
        if lower and lower < start:
            lower[-1] += 1
        if lower < upper:
            if start < lower:
                yield start, lower
            for item in self._range(lower, upper):
                yield item
            if upper < stop:
                yield upper, stop
        else:
            yield start, stop
    def range(self, start, stop, lower=True, upper=False):
        """Return optimal union of date range queries.
        May produce invalid dates, but the query is still correct.
        """
        dates = (map(float, self.split(str(date))) for date in (start, stop))
        items = []
        for dates in self._range(*dates):
            items.append(tuple(self.join(map('{0:02n}'.format, date)) for date in dates))
        queries = [PrefixField.range(self, str(start), items[0][1], lower=lower)]
        queries += [PrefixField.range(self, *item) for item in items[1:-1]]
        queries.append(PrefixField.range(self, items[-1][0], str(stop), upper=upper))
        return Query.any(*queries)
    def within(self, days=0, weeks=0, utc=False, **delta):
        """Return date range query within current time and delta.
        If the delta is an exact number of days, then dates will be used.
        
        :param days, weeks: number of days to offset from today
        :param utc: optionally use utc instead of local time
        :params delta: additional timedelta parameters
        """
        now = datetime.datetime.utcnow() if utc else datetime.datetime.now()
        if not (isinstance(days + weeks, float) or delta):
            now = now.date()
        delta = datetime.timedelta(days, weeks=weeks, **delta)
        return self.range(*sorted([now, now + delta]), upper=True)

class Document(object):
    """Delegated lucene Document.
    Provides mapping interface of field names to values, but duplicate field names are allowed.
    
    :param doc: optional lucene Document
    """
    def __init__(self, doc=None):
        self.doc = lucene.Document() if doc is None else doc
    def add(self, name, value, cls=Field, **params):
        "Add field to document with given parameters."
        for field in cls(name, **params).items(value):
            self.doc.add(field)
    def fields(self):
        "Generate lucene Fields."
        return itertools.imap(lucene.Field.cast_, self.doc.getFields())
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
        if lucene.VERSION <= '2.4.1': # memory leak in string array
            return [field.stringValue() for field in self.doc.getFields(name)]
        return list(self.doc.getValues(name))
    def dict(self, *names, **defaults):
        """Return dict representation of document.
        
        :param names: names of multi-valued fields to return as a list
        :param defaults: include only given fields, using default values as necessary
        """
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
    
    :param searcher: `Searcher`_ which can retrieve documents
    :param ids: ordered doc ids
    :param scores: ordered doc scores
    :param count: total number of hits
    """
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
