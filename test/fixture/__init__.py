"Generate sample fields and documents."

import os
import csv
import itertools
from datetime import datetime

class constitution(object):
    fields = dict.fromkeys(['article', 'amendment', 'date'], {'stored': True, 'tokenized': False})
    fields['text'] = {'storeTermVectors': True, 'storeTermVectorPositions': True, 'storeTermVectorOffsets': True}
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
    fields = dict.fromkeys(['city', 'county', 'state', 'latitude', 'longitude'], {'stored': True, 'indexed': False})
    fields['zipcode'] = {'stored': True, 'tokenized': False}
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
