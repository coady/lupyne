"""
Wrappers for lucene Fields and Documents.
"""

from future_builtins import map, zip
import itertools
import datetime, calendar
import collections
import lucene
from .queries import Query

class Field(object):
    """Saved parameters which can generate lucene Fields given values.
    
    :param name: name of field
    :param store,index,termvector: field parameters, expressed as bools or strs, with lucene defaults
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

class PrefixField(Field):
    """Field which indexes every prefix of a value into a separate component field.
    The customizable component field names are expressed as slices.
    Original value may be stored for convenience.
    
    :param start,stop,step: optional slice parameters of the prefix depths (not indices)
    """
    def __init__(self, name, start=1, stop=None, step=1, index=True, **kwargs):
        Field.__init__(self, name, index=index, **kwargs)
        self.depths = slice(start, stop, step)
    def split(self, text):
        "Return immutable sequence of words from name or value."
        return text
    def join(self, words):
        "Return text from separate words."
        return words
    def getname(self, depth):
        "Return prefix field name for given depth."
        return '{0}[:{1:d}]'.format(self.name, depth)
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
        values = list(map(self.split, values))
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
        depths = self.indices(max(len(self.split(value)) for value in (start, stop) if value is not None))
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
            return lucene.NumericRangeQuery.newDoubleRange(self.name, self.step, start, stop, lower, upper)
        if start is not None:
            start = None if start < lucene.Long.MIN_VALUE else lucene.Long(long(start))
        if stop is not None:
            stop = None if stop > lucene.Long.MAX_VALUE else lucene.Long(long(stop))
        return lucene.NumericRangeQuery.newLongRange(self.name, self.step, start, stop, lower, upper)

class DateTimeField(NumericField):
    """Field which indexes datetimes as a NumericField of timestamps.
    Supports datetimes, dates, and any prefix of time tuples.
    """
    def timestamp(self, date):
        "Return utc timestamp from date or time tuple."
        if isinstance(date, datetime.date):
            return calendar.timegm(date.timetuple()) + getattr(date, 'microsecond', 0) * 1e-6
        return float(calendar.timegm(tuple(date) + (None, 1, 1, 0, 0, 0)[len(date):]))
    def items(self, *dates):
        "Generate lucene NumericFields of timestamps."
        return NumericField.items(self, *map(self.timestamp, dates))
    def range(self, start, stop, lower=True, upper=False):
        "Return NumericRangeQuery of timestamps."
        start, stop = (date and self.timestamp(date) for date in (start, stop))
        return NumericField.range(self, start, stop, lower, upper)
    def prefix(self, date):
        "Return range query which matches the date prefix."
        if isinstance(date, datetime.date):
            date = date.timetuple()[:6 if isinstance(date, datetime.datetime) else 3]
        if len(date) == 2 and date[1] == 12: # month must be valid
            return self.range(date, (date[0]+1, 1))
        return self.range(date, tuple(date[:-1]) + (date[-1]+1,))
    def duration(self, date, days=0, **delta):
        """Return date range query within time span of date.
        
        :param date: origin date or tuple
        :param days,delta: timedelta parameters
        """
        if not isinstance(date, datetime.date):
            date = tuple(date) + (None, 1, 1)[len(date):]
            date = (datetime.datetime if len(date) > 3 else datetime.date)(*date)
        delta = datetime.timedelta(days, **delta)
        return self.range(*sorted([date, date + delta]), upper=True)
    def within(self, days=0, weeks=0, utc=True, **delta):
        """Return date range query within current time and delta.
        If the delta is an exact number of days, then dates will be used.
        
        :param days,weeks: number of days to offset from today
        :param utc: optionally use utc instead of local time
        :param delta: additional timedelta parameters
        """
        date = datetime.datetime.utcnow() if utc else datetime.datetime.now()
        if not (isinstance(days + weeks, float) or delta):
            date = date.date()
        return self.duration(date, days, weeks=weeks, **delta)

class Document(object):
    """Delegated lucene Document.
    Provides mapping interface of field names to values, but duplicate field names are allowed.
    """
    def __init__(self, doc):
        self.doc = doc
    def fields(self):
        "Generate lucene Fields."
        return map(lucene.Field.cast_, self.doc.getFields())
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
    def getlist(self, name):
        "Return list of all values for given field."
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
    :param maxscore: maximum score
    :param fields: optional field selectors
    """
    def __init__(self, searcher, ids, scores, count=None, maxscore=None, fields=None):
        self.searcher = searcher
        self.ids, self.scores = ids, scores
        self.count, self.maxscore = count, maxscore
        self.fields = lucene.MapFieldSelector(fields) if isinstance(fields, collections.Iterable) else fields
    def __len__(self):
        return len(self.ids)
    def __getitem__(self, index):
        id, score = self.ids[index], self.scores[index]
        if isinstance(index, slice):
            return type(self)(self.searcher, id, score, self.count, self.maxscore, self.fields)
        return Hit(self.searcher.doc(id, self.fields), id, score)
    def items(self):
        "Generate zipped ids and scores."
        return zip(self.ids, self.scores)
