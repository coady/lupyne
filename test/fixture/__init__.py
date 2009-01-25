"Generate sample fields and documents."

import os
import csv
import itertools
from datetime import datetime

class data(object):
    @classmethod
    def load(cls, writer, **fields):
        for name, params in cls.fields.items():
            writer.set(name, **params)
        writer.fields.update(fields)
        for doc in cls.docs():
            writer.add(doc)
        writer.commit()

class constitution(data):
    fields = dict.fromkeys(['article', 'amendment', 'date'], {'store': 'yes', 'index': 'not_analyzed'})
    fields['text'] = {}
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

class zipcodes(data):
    fields = dict.fromkeys(['zipcode', 'latitude', 'longitude'], {'store': 'yes', 'index': 'not_analyzed'})
    fields.update(dict.fromkeys(['city', 'county', 'state'], {'store': 'yes', 'index': 'no'}))
    fields['location'] = {'index': 'not_analyzed'}
    @classmethod
    def docs(cls):
        file = open(os.path.join(os.path.dirname(__file__), 'zipcodes.txt'))
        for zipcode, latitude, longitude, state, city, county in csv.reader(file):
            city, county = city.title(), county.title()
            yield {
                'zipcode': zipcode,
                'latitude': '%011f' % float(latitude),
                'longitude': '%011f' % float(longitude),
                'location': ':'.join([state, county, city]),
                'city': city,
                'county': county,
                'state': state,
            }
