"""
Geospatial fields.

Latitude/longitude coordinates are encoded into the quadkeys of MS Virtual Earth,
which are also compatible with Google Maps and OSGEO Tile Map Service.
See http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/.

The quadkeys are then indexed using a prefix tree, creating a cartesian tier of tiles.
"""

import itertools
import lucene
from .globalmaptiles import GlobalMercator
from ..queries import Query
from ..documents import PrefixField

class Tiler(GlobalMercator):
    "Utilities for transforming lat/lngs, projected coordinates, and tile coordinates."
    base = 4
    project = GlobalMercator.LatLonToMeters
    def encode(self, lat, lng, precision):
        "Return tile from latitude, longitude and precision level."
        x, y = self.LatLonToMeters(lat, lng)
        x, y = self.MetersToTile(x, y, precision)
        return self.QuadTree(x, y, precision)
    def decode(self, tile):
        "Return bounding box of tile."
        precision = len(tile)
        n = int(tile, self.base)
        point = [0, 0]
        for i in range(precision):
            for j in (0, 1):
                point[j] |= (n & 1) << i
                n >>= 1
        x, y = point
        return self.TileLatLonBounds(x, 2**precision - 1 - y, precision)
    def walk(self, bottomleft, topright, precision, limit=float('inf')):
        "Generate tile keys which span bounding box, adjusting precision to limit the total count."
        corners = bottomleft, topright
        for precision in range(precision, 0, -1):
            (left, bottom), (right, top) = (self.MetersToTile(x, y, precision) for x, y in corners)
            if (right+1-left) * (top+1-bottom) <= limit:
                break
        assert left >= 0 and bottom >= 0, "bounding box out of global range: {0}".format(corners)
        for x in range(left, right+1):
            for y in range(bottom, top+1):
                yield self.QuadTree(x, y, precision)
    def zoom(self, tiles):
        "Return reduced number of tiles, by zooming out where all sub-tiles are present."
        result, keys = [], []
        for key, values in itertools.groupby(sorted(tiles), lambda tile: tile[:-1]):
            values = list(values)
            if len(values) >= self.base:
                keys.append(key)
            else:
                result += values
        return result + (keys and self.zoom(keys))

class PointField(PrefixField, Tiler):
    """Geospatial points, which create a tiered index of tiles.
    Points must still be stored if exact distances are required upon retrieval.
    
    :param precision: zoom level, i.e., length of encoded value
    """
    def __init__(self, name, precision=30, **kwargs):
        Tiler.__init__(self)
        PrefixField.__init__(self, name, **kwargs)
        self.precision = precision
    def items(self, *points):
        "Generate tiles from points (lng, lat)."
        tiles = set(self.encode(lat, lng, self.precision) for lng, lat in points)
        return PrefixField.items(self, *sorted(tiles))
    def near(self, lng, lat, precision=None):
        "Return prefix query for point at given precision."
        return self.prefix(self.encode(lat, lng, precision or self.precision))
    def within(self, lng, lat, distance, limit=Tiler.base):
        """Return prefix queries for any tiles which could be within distance of given point.
        
        :param lng, lat: point
        :param distance: search radius in meters
        :param limit: maximum number of tiles to consider
        """
        x, y = self.project(lat, lng)
        corners = (x-distance, y-distance), (x+distance, y+distance)
        tiles = self.zoom(self.walk(*corners, precision=self.precision, limit=limit))
        return Query.any(*map(self.prefix, tiles))

class PolygonField(PointField):
    """PointField which implicitly supports polygons (technically linear rings of points).
    Differs from points in that all necessary tiles are included to match the points' boundary.
    As with PointField, the tiered tiles are a search optimization, not a distance calculator.
    """
    def items(self, *polygons):
        "Generate all covered tiles from polygons."
        for points in polygons:
            xs, ys = zip(*(self.project(lat, lng) for lng, lat in points))
            corners = (min(xs), min(ys)), (max(xs), max(ys))
            tiles = self.walk(*corners, precision=self.precision)
            for field in PrefixField.items(self, *tiles):
                yield field
