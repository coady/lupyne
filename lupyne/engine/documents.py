"""
Wrappers for lucene Fields and Documents.
"""

from future_builtins import map
import datetime, calendar
import operator
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
            if isinstance(value, basestring):
                field = lucene.Field(self.name, value, self.store, self.index, self.termvector)
            elif isinstance(value, lucene.JArray_byte):
                field = lucene.Field(self.name, value)
            else:
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
        self.names = tuple(self.values(name))
    def values(self, value):
        "Generate component field values in order."
        value = value.split(self.sep)
        for index in range(1, len(value) + 1):
            yield self.sep.join(value[:index])
    def items(self, *values):
        "Generate indexed component fields."
        if self.store.stored:
            for value in values:
                yield lucene.Field(self.name, value, self.store, lucene.Field.Index.NO)
        for value in values:
            for index, text in enumerate(self.values(value)):
                yield lucene.Field(self.names[index], text, lucene.Field.Store.NO, self.index, self.termvector)
    def prefix(self, value):
        "Return prefix query of the closest possible prefixed field."
        index = value.count(self.sep)
        return Query.prefix(self.names[index], value)
    def range(self, start, stop, lower=True, upper=False):
        "Return range query of the closest possible prefixed field."
        index = max(value.count(self.sep) for value in (start, stop) if value is not None)
        return Query.range(self.names[index], start, stop, lower, upper)

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
    def numeric(self, cls, start, stop, lower, upper):
        if isinstance(start, float) or isinstance(stop, float):
            start, stop = (value if value is None else lucene.Double(value) for value in (start, stop))
            return cls.newDoubleRange(self.name, self.step, start, stop, lower, upper)
        if start is not None:
            start = None if start < lucene.Long.MIN_VALUE else lucene.Long(long(start))
        if stop is not None:
            stop = None if stop > lucene.Long.MAX_VALUE else lucene.Long(long(stop))
        return cls.newLongRange(self.name, self.step, start, stop, lower, upper)
    def range(self, start, stop, lower=True, upper=False):
        "Return lucene NumericRangeQuery."
        return self.numeric(lucene.NumericRangeQuery, start, stop, lower, upper)
    def term(self, value):
        "Return range query to match single term."
        return self.range(value, value, upper=True)
    def filter(self, start, stop, lower=True, upper=False):
        "Return lucene NumericRangeFilter."
        return self.numeric(lucene.NumericRangeFilter, start, stop, lower, upper)

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

class Document(dict):
    "Multimapping of field names to values, but default getters return the first value."
    def __init__(self, doc):
        for field in doc.getFields():
            self.setdefault(field.name(), []).append(field.binaryValue.string_ if field.binary else field.stringValue())
    def __getitem__(self, name):
        return dict.__getitem__(self, name)[0]
    def get(self, name, default=None):
        return dict.get(self, name, [default])[0]
    def getlist(self, name):
        "Return list of all values for given field."
        return dict.get(self, name, [])
    def dict(self, *names, **defaults):
        """Return dict representation of document.
        
        :param names: names of multi-valued fields to return as a list
        :param defaults: include only given fields, using default values as necessary
        """
        defaults.update((name, self[name]) for name in (defaults or self) if name in self)
        defaults.update((name, self.getlist(name)) for name in names)
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
    :param scoredocs: lucene ScoreDocs
    :param count: total number of hits
    :param maxscore: maximum score
    :param fields: optional field selectors
    """
    def __init__(self, searcher, scoredocs, count=None, maxscore=None, fields=None):
        self.searcher, self.scoredocs = searcher, scoredocs
        self.count, self.maxscore = count, maxscore
        self.fields = lucene.MapFieldSelector(fields) if isinstance(fields, collections.Iterable) else fields
    def __len__(self):
        return len(self.scoredocs)
    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            assert step == 1, 'slice step is not supported'
            scoredocs = self.scoredocs[start:stop] if stop - start < len(self) else self.scoredocs
            return type(self)(self.searcher, scoredocs, self.count, self.maxscore, self.fields)
        scoredoc = self.scoredocs[index]
        return Hit(self.searcher.doc(scoredoc.doc, self.fields), scoredoc.doc, scoredoc.score)
    @property
    def ids(self):
        return map(operator.attrgetter('doc'), self.scoredocs)
    @property
    def scores(self):
        return map(operator.attrgetter('score'), self.scoredocs)
    def items(self):
        "Generate zipped ids and scores."
        return map(operator.attrgetter('doc', 'score'), self.scoredocs)
    def groupby(self, func):
        "Return ordered list of `Hits`_ grouped by value of function applied to doc ids."
        groups = {}
        for scoredoc in self.scoredocs:
            value = func(scoredoc.doc)
            try:
                group = groups[value]
            except KeyError:
                group = groups[value] = type(self)(self.searcher, [], fields=self.fields)
                group.index, group.value = len(groups), value
            group.scoredocs.append(scoredoc)
        return sorted(groups.values(), key=lambda group: group.__dict__.pop('index'))
    def filter(self, func):
        "Return `Hits`_ filtered by function applied to doc ids."
        scoredocs = [scoredoc for scoredoc in self.scoredocs if func(scoredoc.doc)]
        return type(self)(self.searcher, scoredocs, fields=self.fields)
    def sorted(self, key, reverse=False):
        "Return `Hits`_ sorted by key function applied to doc ids."
        scoredocs = sorted(self.scoredocs, key=lambda scoredoc: key(scoredoc.doc), reverse=reverse)
        return type(self)(self.searcher, scoredocs, self.count, self.maxscore, self.fields)

class Grouping(object):
    """Delegated lucene SearchGroups with optimized faceting.
    
    :param searcher: `IndexSearcher`_ which can retrieve documents
    :param field: unique field name to group by
    :param query: lucene Query to select groups
    :param count: maximum number of groups
    :param sort: lucene Sort to order groups
    """
    def __init__(self, searcher, field, query=None, count=None, sort=None):
        self.searcher, self.field = searcher, field
        self.query = query or lucene.MatchAllDocsQuery()
        self.sort = sort or lucene.Sort.RELEVANCE
        if count is None:
            collector = lucene.TermAllGroupsCollector(field)
            lucene.IndexSearcher.search(self.searcher, self.query, collector)
            count = collector.groupCount
        collector = lucene.TermFirstPassGroupingCollector(field, self.sort, count)
        lucene.IndexSearcher.search(self.searcher, self.query, collector)
        self.searchgroups = collector.getTopGroups(0, False).of_(lucene.SearchGroup)
    def __len__(self):
        return self.searchgroups.size()
    def __iter__(self):
        for searchgroup in self.searchgroups:
            yield searchgroup.groupValue.toString()
    def facets(self, filter):
        "Generate field values and counts which match given filter."
        collector = lucene.TermSecondPassGroupingCollector(self.field, self.searchgroups, self.sort, self.sort, 1, False, False, False)
        lucene.IndexSearcher.search(self.searcher, self.query, filter, collector)
        for groupdocs in collector.getTopGroups(0).groups:
            yield groupdocs.groupValue.toString(), groupdocs.totalHits
    def groups(self, count=1, sort=None, scores=False, maxscore=False):
        """Generate grouped `Hits`_ from second pass grouping collector.
        
        :param count: maximum number of docs per group
        :param sort: lucene Sort to order docs within group
        :param scores: compute scores for candidate results
        :param maxscore: compute maximum score of all results
        """
        sort = sort or self.sort
        if sort == lucene.Sort.RELEVANCE:
            scores = maxscore = True
        collector = lucene.TermSecondPassGroupingCollector(self.field, self.searchgroups, self.sort, sort, count, scores, maxscore, False)
        lucene.IndexSearcher.search(self.searcher, self.query, collector)
        for groupdocs in collector.getTopGroups(0).groups:
            hits = Hits(self.searcher, groupdocs.scoreDocs, groupdocs.totalHits, groupdocs.maxScore, getattr(self, 'fields', None))
            hits.value = groupdocs.groupValue.toString()
            yield hits
