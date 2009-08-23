"Generate sample fields and documents."

import os
import csv
import itertools
from datetime import datetime

class constitution(object):
    fields = dict.fromkeys(['article', 'amendment', 'date'], {'store': 'yes', 'index': 'not_analyzed'})
    fields['text'] = {'termvector': 'with_positions_offsets'}
    @classmethod
    def docs(cls):
        file = open(os.path.join(os.path.dirname(__file__), 'constitution.txt'))
        items = itertools.groupby(file, lambda l: l.startswith('Article ') or l.startswith('Amendment '))
        for flag, (header,) in items:
            flag, lines = next(items)
            header, num = header.rstrip('.\n').split(None, 1)
            fields = {header.lower(): num, 'text': ''.join(lines)}
            if header == 'Amendment':
                num, date = num.split()
                date = datetime.strptime(date, '%m/%d/%Y').date()
                fields.update({header.lower(): num, 'date': str(date)})
            yield fields

class zipcodes(object):
    fields = dict.fromkeys(['city', 'county', 'state', 'latitude', 'longitude'], {'store': 'yes', 'index': 'no'})
    fields['zipcode'] = {'store': 'yes', 'index': 'not_analyzed'}
    @classmethod
    def docs(cls):
        file = open(os.path.join(os.path.dirname(__file__), 'zipcodes.txt'))
        for zipcode, latitude, longitude, state, city, county in csv.reader(file):
            yield {
                'zipcode': zipcode,
                'latitude': float(latitude),
                'longitude': float(longitude),
                'city': city.title(),
                'county': county.title(),
                'state': state,
            }
