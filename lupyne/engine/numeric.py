"""
Wrappers for lucene NumericFields, available since version 2.9.
Alternative implementations of spatial fields.
"""

import lucene
from .documents import Field
from . import spatial

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
            stop = None if stop > lucene.Long.MAX_VALUE else lucene.Long(long(stop))
        return lucene.NumericRangeQuery.newLongRange(self.name, start, stop, lower, upper)

class PointField(spatial.SpatialField, NumericField):
    __doc__ = spatial.PointField.__doc__
    items = spatial.PointField.items.im_func
    def tiles(self, points):
        "Generate tile values from points (lng, lat)."
        for tile in spatial.SpatialField.tiles(self, points):
            yield int(tile, self.base)
    def prefix(self, tile):
        "Return range query which is equivalent to the prefix of the tile."
        shift = self.base ** (self.precision - len(tile))
        value = int(tile, self.base) * shift
        return self.range(value, value + shift)

class PolygonField(PointField):
    __doc__ = spatial.PolygonField.__doc__
    items = spatial.PolygonField.items.im_func
