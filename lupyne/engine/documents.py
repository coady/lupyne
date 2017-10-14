"""
Wrappers for lucene Fields and Documents.
"""

from future_builtins import map
import datetime
import calendar
import operator
import lucene  # noqa
from java.lang import Double, Float, Long, Number, Object
from java.util import Arrays, HashSet
from org.apache.lucene import document, index, search, util
from org.apache.lucene.search import grouping
from .queries import Query
FieldType = document.FieldType


class Field(FieldType):
    """Saved parameters which can generate lucene Fields given values.

    :param name: name of field
    :param boost: boost factor
    :param stored, indexed, settings: lucene FieldType attributes
    """
    docValueType = property(FieldType.docValueType, FieldType.setDocValueType)
    indexed = property(FieldType.indexed, FieldType.setIndexed)
    indexOptions = property(FieldType.indexOptions, FieldType.setIndexOptions)
    numericPrecisionStep = property(FieldType.numericPrecisionStep, FieldType.setNumericPrecisionStep)
    numericType = property(FieldType.numericType, FieldType.setNumericType)
    omitNorms = property(FieldType.omitNorms, FieldType.setOmitNorms)
    stored = property(FieldType.stored, FieldType.setStored)
    storeTermVectorOffsets = property(FieldType.storeTermVectorOffsets, FieldType.setStoreTermVectorOffsets)
    storeTermVectorPayloads = property(FieldType.storeTermVectorPayloads, FieldType.setStoreTermVectorPayloads)
    storeTermVectorPositions = property(FieldType.storeTermVectorPositions, FieldType.setStoreTermVectorPositions)
    storeTermVectors = property(FieldType.storeTermVectors, FieldType.setStoreTermVectors)
    tokenized = property(FieldType.tokenized, FieldType.setTokenized)

    properties = {name for name in locals() if not name.startswith('__')}
    types = {int: 'long', float: 'double'}
    types.update(NUMERIC='long', BINARY='bytes', SORTED='string', SORTED_NUMERIC='long', SORTED_SET='string')

    def __init__(self, name, boost=1.0, docValueType='', indexOptions='', numericType='', **settings):
        super(Field, self).__init__()
        self.name, self.boost = name, boost
        for name in settings:
            setattr(self, name, settings[name])
        if indexOptions:
            self.indexOptions = getattr(index.FieldInfo.IndexOptions, indexOptions.upper())
        if numericType:
            self.numericType = getattr(FieldType.NumericType, numericType.upper())
            self.numericClass = getattr(document, numericType.title() + 'Field')
        if docValueType:
            self.docValueType = getattr(index.FieldInfo.DocValuesType, docValueType.upper())
            self.docValueClass = getattr(document, docValueType.title().replace('_', '') + 'DocValuesField')
            if (self.stored or self.indexed or self.numericType):
                settings = self.settings
                del settings['docValueType']
                self.docValueLess = Field(self.name, **settings)
        assert self.stored or self.indexed or self.docValueType or self.numericType

    @classmethod
    def String(cls, name, indexed=True, tokenized=False, omitNorms=True, indexOptions='DOCS_ONLY', **settings):
        """Return Field with default settings for strings."""
        return cls(name, indexed=indexed, tokenized=tokenized, omitNorms=omitNorms, indexOptions=indexOptions, **settings)

    @classmethod
    def Text(cls, name, indexed=True, **settings):
        """Return Field with default settings for text."""
        return cls(name, indexed=indexed, **settings)

    @property
    def settings(self):
        """dict representation of settings"""
        defaults = FieldType()
        result = {}
        for name in Field.properties:
            value = getattr(self, name)
            if value != getattr(defaults, name)():
                result[name] = value if isinstance(value, int) else str(value)
        return result

    def items(self, *values):
        """Generate lucene Fields suitable for adding to a document."""
        if self.docValueType:
            types = {int: long, float: util.NumericUtils.doubleToSortableLong}
            for value in values:
                yield self.docValueClass(self.name, types.get(type(value), util.BytesRef)(value))
            self = getattr(self, 'docValueLess', self)
        if self.numericType:
            types = {int: long, float: float}
            for value in values:
                yield self.numericClass(self.name, types[type(value)](value), self)
        elif self.stored or self.indexed:
            for value in values:
                field = document.Field(self.name, value, self)
                field.setBoost(self.boost)
                yield field


class NestedField(Field):
    """Field which indexes every component into its own field.

    Original value may be stored for convenience.

    :param sep: field separator used on name and values
    """
    def __init__(self, name, sep='.', **settings):
        Field.__init__(self, name, **Field.String(name, **settings).settings)
        self.sep = sep
        self.names = tuple(self.values(name))

    def values(self, value):
        """Generate component field values in order."""
        value = value.split(self.sep)
        for index in range(1, len(value) + 1):
            yield self.sep.join(value[:index])

    def items(self, *values):
        """Generate indexed component fields."""
        for value in values:
            for index, text in enumerate(self.values(value)):
                yield document.Field(self.names[index], text, self)

    def prefix(self, value):
        """Return prefix query of the closest possible prefixed field."""
        index = value.count(self.sep)
        return Query.prefix(self.names[index], value)

    def range(self, start, stop, lower=True, upper=False):
        """Return range query of the closest possible prefixed field."""
        index = max(value.count(self.sep) for value in (start, stop) if value is not None)
        return Query.range(self.names[index], start, stop, lower, upper)


class NumericField(Field):
    """Field which indexes numbers in a prefix tree.

    :param name: name of field
    :param type: optional int, float, or lucene NumericType string
    """
    def __init__(self, name, numericType, indexed=True, omitNorms=True, indexOptions='DOCS_ONLY', **settings):
        settings['numericType'] = self.types.get(numericType, numericType)
        Field.__init__(self, name, indexed=indexed, omitNorms=omitNorms, indexOptions=indexOptions, **settings)

    def range(self, start, stop, lower=True, upper=False):
        """Return lucene NumericRangeQuery."""
        step = self.numericPrecisionStep
        if isinstance(start, float) or isinstance(stop, float):
            start, stop = (value if value is None else Double(value) for value in (start, stop))
            return search.NumericRangeQuery.newDoubleRange(self.name, step, start, stop, lower, upper)
        if start is not None:
            start = None if start < Long.MIN_VALUE else Long(long(start))
        if stop is not None:
            stop = None if stop > Long.MAX_VALUE else Long(long(stop))
        return search.NumericRangeQuery.newLongRange(self.name, step, start, stop, lower, upper)

    def term(self, value):
        """Return range query to match single term."""
        return self.range(value, value, upper=True)


class DateTimeField(NumericField):
    """Field which indexes datetimes as a NumericField of timestamps.

    Supports datetimes, dates, and any prefix of time tuples.
    """
    def __init__(self, name, **settings):
        NumericField.__init__(self, name, float, **settings)

    @classmethod
    def timestamp(cls, date):
        """Return utc timestamp from date or time tuple."""
        if isinstance(date, datetime.date):
            return calendar.timegm(date.timetuple()) + getattr(date, 'microsecond', 0) * 1e-6
        return float(calendar.timegm(tuple(date) + (None, 1, 1, 0, 0, 0)[len(date):]))

    def items(self, *dates):
        """Generate lucene NumericFields of timestamps."""
        return NumericField.items(self, *map(self.timestamp, dates))

    def range(self, start, stop, lower=True, upper=False):
        """Return NumericRangeQuery of timestamps."""
        start, stop = (date and self.timestamp(date) for date in (start, stop))
        return NumericField.range(self, start, stop, lower, upper)

    def prefix(self, date):
        """Return range query which matches the date prefix."""
        if isinstance(date, datetime.date):
            date = date.timetuple()[:6 if isinstance(date, datetime.datetime) else 3]
        if len(date) == 2 and date[1] == 12:  # month must be valid
            return self.range(date, (date[0] + 1, 1))
        return self.range(date, tuple(date[:-1]) + (date[-1] + 1,))

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
    """Multimapping of field names to values, but default getters return the first value."""
    def __init__(self, doc):
        for field in doc.iterator():
            value = convert(field.numericValue() or field.stringValue() or field.binaryValue())
            self.setdefault(field.name(), []).append(value)

    def __getitem__(self, name):
        return dict.__getitem__(self, name)[0]

    def get(self, name, default=None):
        return dict.get(self, name, [default])[0]

    def getlist(self, name):
        """Return list of all values for given field."""
        return dict.get(self, name, [])

    def dict(self, *names, **defaults):
        """Return dict representation of document.

        :param names: names of multi-valued fields to return as a list
        :param defaults: include only given fields, using default values as necessary
        """
        defaults.update((name, self[name]) for name in (defaults or self) if name in self)
        defaults.update((name, self.getlist(name)) for name in names)
        return defaults


def convert(value):
    """Return python object from java Object."""
    if util.BytesRef.instance_(value):
        return util.BytesRef.cast_(value).utf8ToString()
    if not Number.instance_(value):
        return value.toString() if Object.instance_(value) else value
    value = Number.cast_(value)
    return value.doubleValue() if Float.instance_(value) or Double.instance_(value) else int(value.longValue())


class Hit(Document):
    """A Document from a search result, with :attr:`id`, :attr:`score`, and optional sort :attr:`keys`."""
    def __init__(self, doc, id, score, keys=()):
        Document.__init__(self, doc)
        self.id, self.score = id, score
        self.keys = tuple(map(convert, keys))

    def dict(self, *names, **defaults):
        """Return dict representation of document with __id__, __score__, and any sort __keys__."""
        result = Document.dict(self, *names, **defaults)
        result.update(__id__=self.id, __score__=self.score)
        if self.keys:
            result['__keys__'] = self.keys
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
        self.fields = fields

    def select(self, *fields):
        """Only load selected fields."""
        self.fields = HashSet(Arrays.asList(fields))

    def __len__(self):
        return len(self.scoredocs)

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            assert step == 1, 'slice step is not supported'
            scoredocs = self.scoredocs[start:stop] if stop - start < len(self) else self.scoredocs
            return type(self)(self.searcher, scoredocs, self.count, self.maxscore, self.fields)
        scoredoc = self.scoredocs[index]
        keys = search.FieldDoc.cast_(scoredoc).fields if search.FieldDoc.instance_(scoredoc) else ()
        doc = self.searcher.doc(scoredoc.doc, self.fields)
        return Hit(doc, scoredoc.doc, scoredoc.score, keys)

    @property
    def ids(self):
        return map(operator.attrgetter('doc'), self.scoredocs)

    @property
    def scores(self):
        return map(operator.attrgetter('score'), self.scoredocs)

    def items(self):
        """Generate zipped ids and scores."""
        return map(operator.attrgetter('doc', 'score'), self.scoredocs)

    def groupby(self, func, count=None, docs=None):
        """Return ordered list of `Hits`_ grouped by value of function applied to doc ids.
        Optionally limit the number of groups and docs per group."""
        groups = {}
        for scoredoc in self.scoredocs:
            value = func(scoredoc.doc)
            try:
                group = groups[value]
            except KeyError:
                group = groups[value] = type(self)(self.searcher, [], fields=self.fields)
                group.index, group.value = len(groups), value
            group.scoredocs.append(scoredoc)
        groups = sorted(groups.values(), key=lambda group: group.__dict__.pop('index'))
        for group in groups:
            group.count, group.maxscore = len(group), max(group.scores)
            group.scoredocs = group.scoredocs[:docs]
        return Groups(self.searcher, groups[:count], len(groups), self.maxscore, self.fields)

    def filter(self, func):
        """Return `Hits`_ filtered by function applied to doc ids."""
        scoredocs = [scoredoc for scoredoc in self.scoredocs if func(scoredoc.doc)]
        return type(self)(self.searcher, scoredocs, fields=self.fields)

    def sorted(self, key, reverse=False):
        """Return `Hits`_ sorted by key function applied to doc ids."""
        scoredocs = sorted(self.scoredocs, key=lambda scoredoc: key(scoredoc.doc), reverse=reverse)
        return type(self)(self.searcher, scoredocs, self.count, self.maxscore, self.fields)


class Groups(object):
    """Sequence of grouped `Hits`_."""
    select = Hits.__dict__['select']

    def __init__(self, searcher, groupdocs, count=None, maxscore=None, fields=None):
        self.searcher, self.groupdocs = searcher, groupdocs
        self.count, self.maxscore = count, maxscore
        self.fields = fields

    def __len__(self):
        return len(self.groupdocs)

    def __getitem__(self, index):
        hits = groupdocs = self.groupdocs[index]
        if isinstance(groupdocs, grouping.GroupDocs):
            hits = Hits(self.searcher, groupdocs.scoreDocs, groupdocs.totalHits, groupdocs.maxScore)
            hits.value = convert(groupdocs.groupValue)
        hits.fields = self.fields
        return hits

    @property
    def facets(self):
        """mapping of field values and counts"""
        return {hits.value: hits.count for hits in self}


class GroupingSearch(grouping.GroupingSearch):
    """Inherited lucene GroupingSearch with optimized faceting.

    :param field: unique field name to group by
    :param sort: lucene Sort to order groups and docs
    :param cache: use unlimited caching
    :param attrs: additional attributes to set
    """
    def __init__(self, field, sort=None, cache=True, **attrs):
        grouping.GroupingSearch.__init__(self, field)
        self.field = field
        if sort:
            self.groupSort = self.sortWithinGroup = sort
            self.fillSortFields = True
        if cache:
            self.setCachingInMB(float('inf'), True)
        for name in attrs:
            getattr(type(self), name).__set__(self, attrs[name])

    def __len__(self):
        return self.allMatchingGroups.size()

    def __iter__(self):
        return map(convert, self.allMatchingGroups)

    def search(self, searcher, query, count=None, start=0):
        """Run query and return `Groups`_."""
        if count is None:
            count = sum(search.FieldCache.DEFAULT.getTermsIndex(reader, self.field).valueCount for reader in searcher.readers)
        topgroups = grouping.GroupingSearch.search(self, searcher, query, start, count - start)
        return Groups(searcher, topgroups.groups, topgroups.totalHitCount, topgroups.maxScore)
