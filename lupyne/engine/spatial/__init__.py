"""
Geospatial fields.

Latitude/longitude coordinates are encoded into the quadkeys of MS Virtual Earth,
which are also compatible with Google Maps and OSGEO Tile Map Service.
See http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/.

The quadkeys are then indexed using a prefix tree, creating a cartesian tier of tiles.
"""

import itertools
from .globalmaptiles import GlobalMercator
from ..queries import Query
from ..documents import NumericField

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
            left, bottom = (max(value, 0) for value in self.MetersToTile(x-distance, y-distance, precision))
            right, top = (min(value + 1, 2 ** precision) for value in self.MetersToTile(x+distance, y+distance, precision))
            if (right - left) * (top - bottom) <= limit:
                break
        for i, j in itertools.product(range(left, right), range(bottom, top)):
            left, bottom, right, top = self.TileBounds(i, j, precision)
            dx = min(0, x-left, right-x)
            dy = min(0, y-bottom, top-y)
            if (dx**2 + dy**2) <= distance**2:
                yield self.QuadTree(i, j, precision)

class PointField(NumericField, Tiler):
    """Geospatial points, which create a tiered index of tiles.
    Points must still be stored if exact distances are required upon retrieval.
    
    :param precision: zoom level, i.e., length of encoded value
    """
    def __init__(self, name, precision=30, **kwargs):
        Tiler.__init__(self)
        NumericField.__init__(self, name, **kwargs)
        self.precision = precision
    def items(self, *points):
        "Generate tiles from points (lng, lat)."
        tiles = set(self.encode(lat, lng, self.precision) for lng, lat in points)
        return NumericField.items(self, *(int(tile, self.base) for tile in tiles))
    def ranges(self, tiles):
        "Generate range queries by grouping adjacent tiles."
        precision, = set(map(len, tiles))
        step = self.base ** (self.precision - precision)
        tiles = (int(tile, self.base) * step for tile in tiles)
        start = next(tiles)
        stop = start + step
        for tile in tiles:
            if tile != stop:
                yield self.range(start, stop)
                start = tile
            stop = tile + step
        yield self.range(start, stop)
    def prefix(self, tile):
        "Return range query which is equivalent to the prefix of the tile."
        return next(self.ranges([tile]))
    def near(self, lng, lat, precision=None):
        "Return prefix query for point at given precision."
        return self.prefix(self.encode(lat, lng, precision or self.precision))
    def within(self, lng, lat, distance, limit=Tiler.base):
        """Return range queries for any tiles which could be within distance of given point.
        
        :param lng,lat: point
        :param distance: search radius in meters
        :param limit: maximum number of tiles to consider
        """
        tiles = sorted(self.radiate(lat, lng, distance, self.precision, limit))
        return Query.any(*self.ranges(tiles))

class PolygonField(PointField):
    """PointField which implicitly supports polygons (technically linear rings of points).
    Differs from points in that all necessary tiles are included to match the points' boundary.
    As with PointField, the tiered tiles are a search optimization, not a distance calculator.
    """
    def items(self, *polygons):
        "Generate all covered tiles from polygons."
        tiles = set()
        for points in polygons:
            xs, ys = zip(*(self.project(lat, lng) for lng, lat in points))
            tiles.update(self.walk(min(xs), min(ys), max(xs), max(ys), self.precision))
        return NumericField.items(self, *(int(tile, self.base) for tile in tiles))

class DistanceComparator(Tiler):
    "Distance comparator computed from cached lat/lngs."
    def __init__(self, lng, lat, lngs, lats):
        Tiler.__init__(self)
        self.x, self.y = self.project(lat, lng)
        self.lngs, self.lats = lngs, lats
    def __getitem__(self, id):
        x, y = self.project(self.lats[id], self.lngs[id])
        return ((x - self.x)**2 + (y - self.y)**2) ** 0.5
