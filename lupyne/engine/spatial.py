"""
Geospatial fields.

Latitude/longitude coordinates are encoded into the quadkeys of MS Virtual Earth,
which are also compatible with Google Maps and OSGEO Tile Map Service.
See http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/.

The quadkeys are then indexed using a prefix tree, creating a cartesian tier of tiles.
"""

import math
import itertools
from .queries import Query
from .documents import NumericField


class Point(tuple):
    "Geodetic coordinates (EPSG:4326) in Spherical Mercator (EPSG:900913) format."
    __slots__ = ()
    circumference = 2 * math.pi * 6378137

    def __new__(cls, lng, lat):
        lat = math.log(math.tan((lat + 90) * math.pi / 360.0)) * 180 / math.pi
        return tuple.__new__(cls, (l * cls.circumference / 360 for l in (lng, lat)))

    @property
    def coords(self):
        "Geodetic coordinates (EPSG:4326)"
        lng, lat = (m * 360 / self.circumference for m in self)
        lat = math.atan(math.exp(lat * math.pi / 180.0)) * 360 / math.pi - 90
        return lng, lat

    def distance(self, other):
        "Return euclidean distance between points in meters."
        return sum((i - j) ** 2 for i, j in zip(self, other)) ** 0.5

    def tile(self, zoom):
        "Return enclosing `Tile`_ at given zoom level."
        size = 2 ** zoom
        coords = (int(math.ceil((m / self.circumference + 0.5) * size) - 1) for m in self)
        return Tile(*(min(max(t, 0), size - 1) for t in coords), zoom=zoom)

    def within(self, distance, zoom):
        "Generate sets of tiles with increasing zoom which are within distance of point."
        tiles = [str.__new__(Tile, '')]
        for i in xrange(zoom):
            tiles = [subtile for tile in tiles for subtile in tile.subtiles if subtile.distance(self) <= distance]
            yield tiles


class Tile(str):
    "TMS tile coordinates in QuadTree format."
    __slots__ = ()

    def __new__(cls, x, y, zoom):
        assert 0 <= min(x, y) <= max(x, y) < 2 ** zoom
        indices = zip(*map(('{:0%db}' % zoom).format, (x, y)))
        return str.__new__(cls, ''.join(str(int(i) + 2 * (not int(j))) for i, j in indices))

    def __int__(self):
        return int(self, 4)

    @property
    def coords(self):
        "TMS tile coordinates"
        indices = zip(*(divmod(int(digit), 2) for digit in self))
        y, x = (int(''.join(map(str, t)), 2) for t in indices)
        return x, (2 ** len(self) - 1 - y)

    @property
    def points(self):
        "lower-left and upper-right `Point`_ corners"
        size = Point.circumference / (2 ** len(self))
        point = tuple.__new__(Point, (t * size - Point.circumference / 2 for t in self.coords))
        return point, tuple.__new__(Point, (m + size for m in point))

    @property
    def subtiles(self):
        return [str.__new__(Tile, self + i) for i in '0123']

    def distance(self, point):
        "Return euclidean distance between tile and point in meters."
        return sum(max(0, l - m, m - u) ** 2 for m, l, u in zip(point, *self.points)) ** 0.5

    def walk(self, other):
        "Generate all tiles between corners."
        assert len(self) == len(other)
        (left, right), (bottom, top) = map(sorted, zip(self.coords, other.coords))
        for x, y in itertools.product(xrange(left, right + 1), xrange(bottom, top + 1)):
            yield type(self)(x, y, len(self))


class PointField(NumericField):
    """Geospatial points, which create a tiered index of tiles.
    Points must still be stored if exact distances are required upon retrieval.

    :param precision: zoom level, i.e., length of encoded value
    """
    def __init__(self, name, precision=30, **kwargs):
        NumericField.__init__(self, name, type=int, **kwargs)
        self.precision = precision

    def tile(self, lng, lat):
        return Point(lng, lat).tile(self.precision)

    def items(self, *points):
        "Generate tiles from points (lng, lat)."
        tiles = set(itertools.starmap(self.tile, points))
        return NumericField.items(self, *map(int, tiles))

    def ranges(self, tiles):
        "Generate range queries by grouping adjacent tiles."
        precision, = set(map(len, tiles))
        step = 4 ** (self.precision - precision)
        tiles = (int(tile, 4) * step for tile in tiles)
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
        return self.prefix(self.tile(lng, lat)[:precision])

    def within(self, lng, lat, distance, limit=4):
        """Return range queries for any tiles which could be within distance of given point.
        
        :param lng,lat: point
        :param distance: search radius in meters
        :param limit: maximum number of tiles to consider
        """
        sets = Point(lng, lat).within(distance, self.precision)
        tiles = list(itertools.takewhile(lambda tiles: len(tiles) <= limit, sets))[-1]
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
            lngs, lats = zip(*points)
            lower, upper = self.tile(min(lngs), min(lats)), self.tile(max(lngs), max(lats))
            tiles.update(lower.walk(upper))
        return NumericField.items(self, *map(int, tiles))


class DistanceComparator(object):
    "Distance comparator computed from cached lat/lngs."
    def __init__(self, lng, lat, lngs, lats):
        self.point = Point(lng, lat)
        self.lngs, self.lats = lngs, lats

    def __getitem__(self, id):
        return self.point.distance(Point(self.lngs[id], self.lats[id]))
