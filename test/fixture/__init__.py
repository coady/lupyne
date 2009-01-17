"Split U.S. Constitution into sample documents."

import os
import itertools

def fields():
    "Generate fields with suggested settings."
    yield 'text', {}
    yield 'article', {'store': 'yes', 'index': 'not_analyzed'}
    yield 'amendment', {'store': 'yes', 'index': 'not_analyzed'}

def docs():
    "Generate sample documents."
    file = open(os.path.join(os.path.dirname(__file__), 'constitution.txt'))
    items = itertools.groupby(file, lambda l: l.startswith('Article ') or l.startswith('Amendment '))
    for flag, (header,) in items:
        flag, lines = next(items)
        header, num = header.rstrip('.\n').split()
        yield {header.lower(): num, 'text': ''.join(lines)}
