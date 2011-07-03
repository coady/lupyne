"""
Wrappers for lucene Fields and Documents.
"""

from future_builtins import map, zip
import datetime, calendar
import collections
import lucene
from .queries import Query

class Field(object):
    """Saved parameters which can generate lucene Fields given values.
    
    :param name: name of field
    :param store,index,termvector: field parameters, expressed as bools or strs, with lucene defaults
    :param analyzed,omitNorms: additional index boolean settings
    :param withPositions,withOffsets: additional termvector boolean settings
    :param attrs: additional attributes to set on the field
    """
    def __init__(self, name, store=False, index='analyzed', termvector=False, analyzed=False, omitNorms=False, withPositions=False, withOffsets=False, **attrs):
        self.name, self.attrs = name, attrs
        if isinstance(store, bool):
            store = 'yes' if store else 'no'
        self.store = lucene.Field.Store.valueOf(store.upper())
        if isinstance(index, bool):
            self.index = lucene.Field.Index.toIndex(index, analyzed, omitNorms)
        else:
            self.index = lucene.Field.Index.valueOf(index.upper())
        if isinstance(termvector, bool):
            self.termvector = lucene.Field.TermVector.toTermVector(termvector, withOffsets, withPositions)
        else:
            self.termvector = lucene.Field.TermVector.valueOf(termvector.upper())
        next(Field.items(self, ' ')) # validate settings
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

class NestedField(Field):
    """Field which indexes every component into its own field.
    Original value may be stored for convenience.
    
    :param sep: field separator used on name and values
    """
    def __init__(self, name, sep='.', index=True, **kwargs):
        Field.__init__(self, name, index=index, **kwargs)
        self.sep = sep
        self.names = self.split(name)
    def split(self, value):
        "Return sequence of words from name or value."
        return tuple(value.split(self.sep))
    def join(self, words):
        "Return text from separate words."
        return self.sep.join(words)
    def getname(self, index):
        "Return prefix of field name."
        return self.join(self.names[:index])
    def items(self, *values):
        "Generate indexed component fields."
        if self.store.stored:
            for value in values:
                yield lucene.Field(self.name, value, self.store, lucene.Field.Index.NO)
        for value in values:
            value = self.split(value)
            for index in range(1, len(value) + 1):
                yield lucene.Field(self.getname(index), self.join(value[:index]), lucene.Field.Store.NO, self.index, self.termvector)
    def prefix(self, value):
        "Return prefix query of the closest possible prefixed field."
        index = len(self.split(value))
        return Query.prefix(self.getname(index), value)
    def range(self, start, stop, lower=True, upper=False):
        "Return range query of the closest possible prefixed field."
        index = max(len(self.split(value)) for value in (start, stop) if value is not None)
        return Query.range(self.getname(index), start, stop, lower, upper)

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
            date = datetime.datetime(*(tuple(date) + (None, 1, 1)[len(date):]))
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
    def __len__(self):
        return self.doc.getFields().size()
    def __contains__(self, name):
        return self.doc[name] is not None
    def __iter__(self):
        for field in self.doc.getFields():
            yield field.name()
    def items(self):
        "Generate name, value pairs for all fields."
        for field in self.doc.getFields():
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
    
    :param searcher: `IndexSearcher`_ which can retrieve documents
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
    def groupby(self, name, type='string', parser=None):
        "Return ordered list of `Hits`_ grouped by cached comparator value."
        groups, values = {}, self.searcher.comparator(name, type, parser)
        for id, score in self.items():
            value = values[id]
            try:
                group = groups[value]
            except KeyError:
                group = groups[value] = object.__class__(self)(self.searcher, [], [], fields=self.fields)
                group.index, group.value = len(groups), value
            group.ids.append(id)
            group.scores.append(score)
        return sorted(groups.values(), key=lambda group: group.__dict__.pop('index'))
