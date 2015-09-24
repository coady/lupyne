"""
Output a kml file (for Google Earth) which visualizes a spatial tile search.

Default searches within 1 kilometer of Griffith Observatory.
Reports the number of found tiles, precision level,
final number of grouped tiles, and the ratio of extra area searched.
Experiment with different tile limits to see search accuracy.
"""

import argparse
import itertools
import math
import os
import sys
from lupyne.engine.spatial import Point, Tile

overlay = '''<GroundOverlay>
<color>7fff0000</color>
<drawOrder>1</drawOrder>
<LatLonBox>
<north>{}</north>
<south>{}</south>
<east>{}</east>
<west>{}</west>
</LatLonBox>
</GroundOverlay>'''

document = '''<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Folder>
<Placemark>
<Point>
<coordinates>
{},{}
</coordinates>
</Point>
</Placemark>
{}
</Folder>
</kml>'''

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--lng', type=float, default='-118.3004', help='longitude [%(default)s]')
parser.add_argument('--lat', type=float, default='34.1184', help='latitude [%(default)s]')
parser.add_argument('--distance', type=float, default='1000', help='search radius in meters [%(default)s]')
parser.add_argument('--tiles', type=int, default='4', help='maximum number of tiles to consider [%(default)s]')

if __name__ == '__main__':
    args = parser.parse_args()
    sets = Point(args.lng, args.lat).within(args.distance, 30)
    tiles = list(itertools.takewhile(lambda tiles: len(tiles) <= args.tiles, sets))[-1]
    print >>sys.stderr, len(tiles), 'tiles at precision', len(tiles[0])

    grouped = []
    while tiles:
        remaining = []
        for key, group in itertools.groupby(tiles, key=lambda tile: tile[:-1]):
            group = list(group)
            if len(group) == 4:
                remaining.append(str.__new__(Tile, key))
            else:
                grouped += group
        tiles = remaining

    overlays = []
    area = 0.0
    for tile in grouped:
        points = tile.points
        width, height = (abs(i - j) for i, j in zip(*points))
        area += width * height
        (west, south), (east, north) = (point.coords for point in points)
        overlays.append(overlay.format(north, south, east, west))
    area /= math.pi * (args.distance) ** 2
    print >>sys.stderr, len(grouped), 'grouped tiles covering', area, 'times the circle area'
    print document.format(args.lng, args.lat, os.linesep.join(overlays))
