import calendar
import collections
import datetime
import operator
from typing import Callable, Iterator, Optional, Sequence
import lucene  # noqa
from java.lang import Long
from java.util import Arrays, HashSet
from org.apache.lucene import document, index, search, util
from org.apache.lucene.search import grouping
from .queries import Query
from .utils import convert

FieldType = document.FieldType


class Field(FieldType):  # type: ignore
    """Saved parameters which can generate lucene Fields given values.

    Args:
        name: name of field
    """

    docValuesType = property(FieldType.docValuesType, FieldType.setDocValuesType)
    indexOptions = property(FieldType.indexOptions, FieldType.setIndexOptions)
    omitNorms = property(FieldType.omitNorms, FieldType.setOmitNorms)
    stored = property(FieldType.stored, FieldType.setStored)
    storeTermVectorOffsets = property(FieldType.storeTermVectorOffsets, FieldType.setStoreTermVectorOffsets)
    storeTermVectorPayloads = property(FieldType.storeTermVectorPayloads, FieldType.setStoreTermVectorPayloads)
    storeTermVectorPositions = property(FieldType.storeTermVectorPositions, FieldType.setStoreTermVectorPositions)
    storeTermVectors = property(FieldType.storeTermVectors, FieldType.setStoreTermVectors)
    tokenized = property(FieldType.tokenized, FieldType.setTokenized)

    properties = {name for name in locals() if not name.startswith('__')}
    types = {int: 'long', float: 'double', str: 'string'}
    types.update(NUMERIC='long', BINARY='string', SORTED='string', SORTED_NUMERIC='long', SORTED_SET='string')
    dimensions = property(
        getattr(FieldType, 'pointDataDimensionCount', getattr(FieldType, 'pointDimensionCount', None)),
        lambda self, count: self.setDimensions(count, Long.BYTES),
    )

    def __init__(self, name: str, docValuesType='', indexOptions='', dimensions=0, **settings):
        super().__init__()
        self.name = name
        for name in self.properties.intersection(settings):
            setattr(self, name, settings.pop(name))
        for name in settings:
            raise AttributeError(f"'Field' object has no property '{name}'")
        if dimensions:
            self.dimensions = dimensions
        if indexOptions:
            self.indexOptions = getattr(index.IndexOptions, indexOptions.upper())
        if docValuesType:
            self.docValuesType = getattr(index.DocValuesType, docValuesType.upper())
            self.docValueClass = getattr(document, docValuesType.title().replace('_', '') + 'DocValuesField')
            if self.stored or self.indexed or self.dimensions:
                settings = self.settings
                del settings['docValuesType']
                self.docValueLess = Field(self.name, **settings)
        assert self.stored or self.indexed or self.docvalues or self.dimensions

    @classmethod
    def String(cls, name: str, tokenized=False, omitNorms=True, indexOptions='DOCS', **settings) -> 'Field':
        """Return Field with default settings for strings."""
        return cls(name, tokenized=tokenized, omitNorms=omitNorms, indexOptions=indexOptions, **settings)

    @classmethod
    def Text(cls, name: str, indexOptions='DOCS_AND_FREQS_AND_POSITIONS', **settings) -> 'Field':
        """Return Field with default settings for text."""
        return cls(name, indexOptions=indexOptions, **settings)

    @property
    def indexed(self):
        return self.indexOptions != index.IndexOptions.NONE

    @property
    def docvalues(self):
        return self.docValuesType != index.DocValuesType.NONE

    @property
    def settings(self) -> dict:
        """dict representation of settings"""
        defaults = FieldType()
        result = {'dimensions': self.dimensions} if self.dimensions else {}
        for name in Field.properties:
            value = getattr(self, name)
            if value != getattr(defaults, name)():
                result[name] = value if isinstance(value, int) else str(value)
        return result

    def items(self, *values) -> Iterator[document.Field]:
        """Generate lucene Fields suitable for adding to a document."""
        if self.docvalues:
            types = {int: int, float: util.NumericUtils.doubleToSortableLong}
            for value in values:
                yield self.docValueClass(self.name, types.get(type(value), util.BytesRef)(value))
            self = getattr(self, 'docValueLess', self)  # type: ignore
        if self.dimensions:
            for value in values:
                if isinstance(value, int):
                    yield document.LongPoint(self.name, int(value))
                else:
                    yield document.DoublePoint(self.name, value)
        if self.indexed:
            for value in values:
                yield document.Field(self.name, value, self)
        elif self.stored:
            for value in values:
                yield document.StoredField(self.name, value)


class NestedField(Field):
    """Field which indexes every component into its own field.

    Original value may be stored for convenience.

    Args:
        sep: field separator used on name and values
    """

    def __init__(self, name: str, sep: str = '.', **settings):
        super().__init__(name, **Field.String(name, **settings).settings)
        self.sep = sep
        self.names = tuple(self.values(name))

    def values(self, value: str) -> Iterator[str]:
        """Generate component field values in order."""
        value = value.split(self.sep)  # type: ignore
        for index in range(1, len(value) + 1):
            yield self.sep.join(value[:index])

    def items(self, *values: str) -> Iterator[document.Field]:
        """Generate indexed component fields."""
        field = getattr(self, 'docValueLess', self)
        for value in values:
            for name, text in zip(self.names, self.values(value)):
                yield document.Field(name, text, field)
                if self.docvalues:
                    yield self.docValueClass(name, util.BytesRef(text))

    def prefix(self, value: str) -> Query:
        """Return prefix query of the closest possible prefixed field."""
        index = value.count(self.sep)
        return Query.prefix(self.names[index], value)

    def range(self, start, stop, lower=True, upper=False) -> Query:
        """Return range query of the closest possible prefixed field."""
        index = max(value.count(self.sep) for value in (start, stop) if value is not None)
        return Query.range(self.names[index], start, stop, lower, upper)


class DateTimeField(Field):
    """Field which indexes datetimes as Point fields of timestamps.

    Supports datetimes, dates, and any prefix of time tuples.
    """

    def __init__(self, name: str, dimensions: int = 1, **settings):
        super().__init__(name, dimensions=dimensions, **settings)

    @classmethod
    def timestamp(cls, date) -> float:
        """Return utc timestamp from date or time tuple."""
        if isinstance(date, datetime.date):
            return calendar.timegm(date.timetuple()) + getattr(date, 'microsecond', 0) * 1e-6
        return float(calendar.timegm(tuple(date) + (None, 1, 1, 0, 0, 0)[len(date) :]))

    def items(self, *dates) -> Iterator[document.Field]:
        """Generate lucene NumericFields of timestamps."""
        return super().items(*map(self.timestamp, dates))

    def range(self, start, stop, **inclusive) -> Query:
        """Return NumericRangeQuery of timestamps."""
        interval = (date and self.timestamp(date) for date in (start, stop))
        return Query.ranges(self.name, interval, **inclusive)

    def prefix(self, date) -> Query:
        """Return range query which matches the date prefix."""
        if isinstance(date, datetime.date):
            date = date.timetuple()[: 6 if isinstance(date, datetime.datetime) else 3]
        if len(date) == 2 and date[1] == 12:  # month must be valid
            return self.range(date, (date[0] + 1, 1))
        return self.range(date, tuple(date[:-1]) + (date[-1] + 1,))

    def duration(self, date, days=0, **delta) -> Query:
        """Return date range query within time span of date.

        Args:
            date: origin date or tuple
            days **delta:: timedelta parameters
        """
        if not isinstance(date, datetime.date):
            date = datetime.datetime(*(tuple(date) + (None, 1, 1)[len(date) :]))
        delta = datetime.timedelta(days, **delta)  # type: ignore
        return self.range(*sorted([date, date + delta]), upper=True)

    def within(self, days=0, weeks=0, utc=True, **delta) -> Query:
        """Return date range query within current time and delta.

        If the delta is an exact number of days, then dates will be used.

        Args:
            days weeks: number of days to offset from today
            utc: optionally use utc instead of local time
            **delta: additional timedelta parameters
        """
        date = datetime.datetime.utcnow() if utc else datetime.datetime.now()
        if not (isinstance(days + weeks, float) or delta):
            date = date.date()  # type: ignore
        return self.duration(date, days, weeks=weeks, **delta)


class SpatialField(Field):
    """Geospatial points, indexed with optional docvalues."""

    def __init__(self, name: str, dimensions: int = 1, **settings):
        super().__init__(name, dimensions=dimensions, **settings)

    def items(self, *points: tuple) -> Iterator[document.Field]:
        """Generate lucene LatLon fields from points (lng, lat)."""
        for lng, lat in points:
            yield document.LatLonPoint(self.name, lat, lng)
        if self.docvalues:  # pragma: no branch
            for lng, lat in points:
                yield document.LatLonDocValuesField(self.name, lat, lng)

    def within(self, lng: float, lat: float, distance: float) -> search.Query:
        """Return range queries for any tiles which could be within distance of given point.

        Args:
            lng lat: point
            distance: search radius in meters
        """
        return document.LatLonPoint.newDistanceQuery(self.name, lat, lng, distance)

    def distances(self, lng: str, lat: str) -> search.Query:
        """Return distance SortField."""
        return document.LatLonDocValuesField.newDistanceSort(self.name, lat, lng)


class Document(dict):
    """Multimapping of field names to values, but default getters return the first value."""

    def __init__(self, doc: document.Document):
        for field in doc.iterator():
            value = convert(field.numericValue() or field.stringValue() or field.binaryValue())
            self.setdefault(field.name(), []).append(value)

    def __getitem__(self, name):
        return super().__getitem__(name)[0]

    def get(self, name: str, default=None):
        return super().get(name, [default])[0]

    def getlist(self, name: str) -> list:
        """Return list of all values for given field."""
        return super().get(name, [])

    def dict(self, *names: str, **defaults) -> dict:
        """Return dict representation of document.

        Args:
            *names: names of multi-valued fields to return as a list
            **defaults: include only given fields, using default values as necessary
        """
        defaults.update((name, self[name]) for name in (defaults or self) if name in self)
        defaults.update((name, self.getlist(name)) for name in names)
        return defaults


class Hit(Document):
    """A Document from a search result, with :attr:`id`, :attr:`score`, and optional :attr:`sortkeys`.

    Note:
        changed in version 2.4: keys renamed to :attr:`sortkeys`
    """

    def __init__(self, doc: document.Document, id: int, score: float, sortkeys=()):
        super().__init__(doc)
        self.id, self.score = id, score
        self.sortkeys = tuple(map(convert, sortkeys))

    def dict(self, *names: str, **defaults) -> dict:
        """Return dict representation of document with __id__, __score__, and any sort __keys__."""
        result = super().dict(*names, **defaults)
        result.update(__id__=self.id, __score__=self.score)
        if self.sortkeys:
            result['__sortkeys__'] = self.sortkeys
        return result


class Hits:
    """Search results: lazily evaluated and memory efficient.

    Provides a read-only sequence interface to hit objects.

    Note:
        changed in version 2.3: maxscore option removed; computed property instead

    Args:
        searcher: [IndexSearcher][lupyne.engine.indexers.IndexSearcher] which can retrieve documents
        scoredocs: lucene ScoreDocs
        count: total number of hits; float indicates estimate
        fields: optional field selectors
    """

    def __init__(self, searcher, scoredocs: Sequence, count=0, fields=None):
        self.searcher, self.scoredocs = searcher, scoredocs
        if hasattr(count, 'relation'):
            cls = int if count.relation == search.TotalHits.Relation.EQUAL_TO else float
            count = cls(count.value)
        self.count, self.fields = count, fields

    def select(self, *fields: str):
        """Only load selected fields."""
        self.fields = HashSet(Arrays.asList(fields))

    def __len__(self):
        return len(self.scoredocs)

    def __getitem__(self, index):
        if isinstance(index, slice):
            scoredocs = list(map(self.scoredocs.__getitem__, range(*index.indices(len(self)))))
            return type(self)(self.searcher, scoredocs, self.count, self.fields)
        scoredoc = self.scoredocs[index]
        keys = search.FieldDoc.cast_(scoredoc).fields if search.FieldDoc.instance_(scoredoc) else ()
        doc = self.searcher.doc(scoredoc.doc, *([self.fields] * bool(self.fields)))
        return Hit(doc, scoredoc.doc, scoredoc.score, keys)

    @property
    def ids(self) -> Iterator[int]:
        return map(operator.attrgetter('doc'), self.scoredocs)

    @property
    def scores(self) -> Iterator[float]:
        return map(operator.attrgetter('score'), self.scoredocs)

    @property
    def maxscore(self) -> float:
        """max score of present hits; not necessarily of all matches"""
        return max(self.scores, default=float('nan'))

    def items(self) -> Iterator[tuple]:
        """Generate zipped ids and scores."""
        return map(operator.attrgetter('doc', 'score'), self.scoredocs)

    def highlights(self, query: search.Query, **fields: int) -> Iterator[dict]:
        """Generate highlighted fields for each hit.

        Args:
            query: lucene Query
            **fields: mapping of fields to maxinum number of passages
        """
        mapping = self.searcher.highlighter.highlightFields(list(fields), query, list(self.ids), list(fields.values()))
        mapping = {field: lucene.JArray_string.cast_(mapping.get(field)) for field in fields}
        return (dict(zip(mapping, values)) for values in zip(*mapping.values()))

    def docvalues(self, field: str, type=None) -> dict:
        """Return mapping of docs to docvalues."""
        return self.searcher.docvalues(field, type).select(self.ids)

    def groupby(self, func: Callable, count: Optional[int] = None, docs: Optional[int] = None) -> 'Groups':
        """Return ordered list of [Hits][lupyne.engine.documents.Hits] grouped by value of function applied to doc ids.

        Optionally limit the number of groups and docs per group.
        """
        groups = collections.OrderedDict()  # type: dict
        for scoredoc in self.scoredocs:
            value = func(scoredoc.doc)
            try:
                group = groups[value]
            except KeyError:
                group = groups[value] = type(self)(self.searcher, [], fields=self.fields)
                group.value = value
            group.scoredocs.append(scoredoc)
        groups = list(groups.values())  # type: ignore
        for group in groups:
            group.count = len(group)
            group.scoredocs = group.scoredocs[:docs]
        return Groups(self.searcher, groups[:count], len(groups), self.fields)

    def filter(self, func: Callable) -> 'Hits':
        """Return [Hits][lupyne.engine.documents.Hits] filtered by function applied to doc ids."""
        scoredocs = [scoredoc for scoredoc in self.scoredocs if func(scoredoc.doc)]
        return type(self)(self.searcher, scoredocs, fields=self.fields)

    def sorted(self, key: Callable, reverse=False) -> 'Hits':
        """Return [Hits][lupyne.engine.documents.Hits] sorted by key function applied to doc ids."""
        scoredocs = sorted(self.scoredocs, key=lambda scoredoc: key(scoredoc.doc), reverse=reverse)
        return type(self)(self.searcher, scoredocs, self.count, self.fields)


class Groups:
    """Sequence of grouped [Hits][lupyne.engine.documents.Hits]."""

    select = Hits.select

    def __init__(self, searcher, groupdocs: Sequence, count: int = 0, fields=None):
        self.searcher, self.groupdocs = searcher, groupdocs
        self.count, self.fields = count, fields

    def __len__(self):
        return len(self.groupdocs)

    def __getitem__(self, index):
        hits = groupdocs = self.groupdocs[index]
        if isinstance(groupdocs, grouping.GroupDocs):
            hits = Hits(self.searcher, groupdocs.scoreDocs, groupdocs.totalHits)
            hits.value = convert(groupdocs.groupValue)
        hits.fields = self.fields
        return hits

    @property
    def facets(self):
        """mapping of field values and counts"""
        return {hits.value: hits.count for hits in self}


class GroupingSearch(grouping.GroupingSearch):
    """Inherited lucene GroupingSearch with optimized faceting.

    Args:
        field: unique field name to group by
        sort: lucene Sort to order groups and docs
        cache: use unlimited caching
        **attrs: additional attributes to set
    """

    def __init__(self, field: str, sort=None, cache=True, **attrs):
        super().__init__(field)
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

    def search(self, searcher, query: search.Query, count: Optional[int] = None, start: int = 0) -> Groups:
        """Run query and return [Groups][lupyne.engine.documents.Groups]."""
        if count is None:
            count = sum(index.DocValues.getSorted(reader, self.field).valueCount for reader in searcher.readers) or 1
        topgroups = super().search(searcher, query, start, count - start)
        return Groups(searcher, topgroups.groups, topgroups.totalHitCount)
