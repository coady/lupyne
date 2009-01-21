"Split U.S. Constitution into sample documents."

import os
import itertools
import datetime

def fields():
    "Generate fields with suggested settings."
    yield 'text', {}
    yield 'article', {'store': 'yes', 'index': 'not_analyzed'}
    yield 'amendment', {'store': 'yes', 'index': 'not_analyzed'}
    yield 'date', {'store': 'yes', 'index': 'not_analyzed'}

def docs():
    "Generate sample documents."
    file = open(os.path.join(os.path.dirname(__file__), 'constitution.txt'))
    items = itertools.groupby(file, lambda l: l.startswith('Article ') or l.startswith('Amendment '))
    for flag, (header,) in items:
        flag, lines = next(items)
        header, num = header.rstrip('.\n').split(None, 1)
        fields = {header.lower(): num, 'text': ''.join(lines)}
        if header == 'Amendment':
            num, date = num.split()
            month, day, year = map(int, date.split('/'))
            date = datetime.date(year, month, day)
            fields.update({header.lower(): num, 'date': str(date)})
        yield fields
