"""
Wrappers for lucene NumericFields.
Alternative implementations of spatial and datetime fields.
"""

from future_builtins import map
import itertools
import datetime, calendar
import lucene
from .documents import Field
from .queries import Query
from . import spatial, documents

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

class PointField(spatial.SpatialField, NumericField):
    __doc__ = spatial.PointField.__doc__
    items = spatial.PointField.__dict__['items']
    def tiles(self, points, span=False):
        """Generate tile values from points (lng, lat).
        
        :param span: cover entire area of points, as if it were a polygon
        """
        for tile in spatial.SpatialField.tiles(self, points, span):
            yield int(tile, self.base)
    def prefix(self, tile):
        "Return range query which is equivalent to the prefix of the tile."
        shift = self.base ** (self.precision - len(tile))
        value = int(tile, self.base) * shift
        return self.range(value, value + shift)
    def within(self, lng, lat, distance, limit=spatial.Tiler.base):
        """Return range queries for any tiles which could be within distance of given point.
        
        :param lng, lat: point
        :param distance: search radius in meters
        :param limit: maximum number of tiles to consider
        """
        tiles = sorted(self.radiate(lat, lng, distance, self.precision, limit))
        precision, = set(map(len, tiles))
        shift = self.base ** (self.precision - precision)
        slices = []
        for tile in tiles:
            tile = int(tile, self.base) * shift
            if slices and slices[-1][1] == tile:
                slices[-1] = slices[-1][0], tile + shift
            else:
                slices.append((tile, tile + shift))
        return Query.any(*itertools.starmap(self.range, slices))

class PolygonField(PointField):
    __doc__ = spatial.PolygonField.__doc__
    items = spatial.PolygonField.__dict__['items']

class DateTimeField(NumericField):
    """Field which indexes datetimes as a NumericField of timestamps.
    Supports datetimes, dates, and any prefix of time tuples.
    """
    duration = documents.DateTimeField.__dict__['duration']
    within = documents.DateTimeField.__dict__['within']
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
