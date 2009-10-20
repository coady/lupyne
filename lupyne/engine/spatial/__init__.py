"""
Geospatial fields.

Latitude/longitude coordinates are encoded into the quadkeys of MS Virtual Earth,
which are also compatible with Google Maps and OSGEO Tile Map Service.
See http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/.

The quadkeys are then indexed using a prefix tree, creating a cartesian tier of tiles.
"""

import itertools, operator
import lucene
from .globalmaptiles import GlobalMercator
from ..queries import Query
from ..documents import Field, PrefixField

class Tiler(GlobalMercator):
    "Utilities for transforming lat/lngs, projected coordinates, and tile coordinates."
    base = 4
    project = GlobalMercator.LatLonToMeters
    def coords(self, tile):
        "Return TMS coordinates of tile."
        n = int(tile, self.base)
        x = y = 0
        for i in range(len(tile)):
            x |= (n & 1) << i
            y |= (n & 2) >> 1 << i
            n >>= 2
        return x, 2**len(tile) - 1 - y
    def encode(self, lat, lng, precision):
        "Return tile from latitude, longitude and precision level."
        x, y = self.LatLonToMeters(lat, lng)
        x, y = self.MetersToTile(x, y, precision)
        return self.QuadTree(x, y, precision)
    def decode(self, tile):
        "Return lat/lng bounding box (bottom, left, top, right) of tile."
        x, y = self.coords(tile)
        return self.TileLatLonBounds(x, y, len(tile))
    def walk(self, left, bottom, right, top, precision):
        "Generate tile keys which span bounding box of meters."
        left, bottom = self.MetersToTile(left, bottom, precision)
        right, top = self.MetersToTile(right, top, precision)
        for i, j in itertools.product(range(left, right+1), range(bottom, top+1)):
            yield self.QuadTree(i, j, precision)
    def radiate(self, lat, lng, distance, precision, limit=float('inf')):
        "Generate tile keys within distance of given point, adjusting precision to limit the number considered."
        x, y = self.LatLonToMeters(lat, lng)
        for precision in range(precision, 0, -1):
            left, bottom = self.MetersToTile(x-distance, y-distance, precision)
            right, top = self.MetersToTile(x+distance, y+distance, precision)
            if (right+1-left) * (top+1-bottom) <= limit:
                break
        for i, j in itertools.product(range(left, right+1), range(bottom, top+1)):
            left, bottom, right, top = self.TileBounds(i, j, precision)
            dx = min(0, x-left, right-x)
            dy = min(0, y-bottom, top-y)
            if (dx**2 + dy**2) ** 0.5 <= distance:
                yield self.QuadTree(i, j, precision)
    def zoom(self, tiles):
        "Return reduced number of tiles, by zooming out where all sub-tiles are present."
        result, keys = [], []
        for key, values in itertools.groupby(sorted(tiles), operator.itemgetter(slice(-1))):
            values = list(values)
            if len(values) >= self.base:
                keys.append(key)
            else:
                result += values
        return result + (keys and self.zoom(keys))

class SpatialField(Field, Tiler):
    """Mixin interface for indexing lat/lngs as a prefix tree of tiles.
    Subclasses should implement items and prefix methods.
    
    :param precision: zoom level, i.e., length of encoded value
    """
    def __init__(self, name, precision=30, **kwargs):
        Tiler.__init__(self)
        super(SpatialField, self).__init__(name, **kwargs)
        self.precision = precision
    def near(self, lng, lat, precision=None):
        "Return prefix query for point at given precision."
        return self.prefix(self.encode(lat, lng, precision or self.precision))
    def within(self, lng, lat, distance, limit=Tiler.base):
        """Return prefix queries for any tiles which could be within distance of given point.
        
        :param lng, lat: point
        :param distance: search radius in meters
        :param limit: maximum number of tiles to consider
        """
        tiles = self.zoom(self.radiate(lat, lng, distance, self.precision, limit))
        return Query.any(*map(self.prefix, tiles))
    def tiles(self, points, span=False):
        """Generate tile values from points (lng, lat).
        
        :param span: cover entire area of points, as if it were a polygon
        """
        if not span:
            return sorted(set(self.encode(lat, lng, self.precision) for lng, lat in points))
        xs, ys = zip(*(self.project(lat, lng) for lng, lat in points))
        return self.walk(min(xs), min(ys), max(xs), max(ys), self.precision)

class PointField(SpatialField, PrefixField):
    """Geospatial points, which create a tiered index of tiles.
    Points must still be stored if exact distances are required upon retrieval.
    """
    def items(self, *points):
        "Generate tiles from points (lng, lat)."
        return super(SpatialField, self).items(*self.tiles(points))

class PolygonField(PointField):
    """PointField which implicitly supports polygons (technically linear rings of points).
    Differs from points in that all necessary tiles are included to match the points' boundary.
    As with PointField, the tiered tiles are a search optimization, not a distance calculator.
    """
    def items(self, *polygons):
        "Generate all covered tiles from polygons."
        tiles = itertools.chain.from_iterable(self.tiles(points, span=True) for points in polygons)
        return super(SpatialField, self).items(*tiles)
