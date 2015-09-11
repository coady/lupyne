import os
import csv
import itertools
import shutil
import tempfile
import warnings
import contextlib
from datetime import datetime
import pytest


@contextlib.contextmanager
def warns(*categories):
    with warnings.catch_warnings(record=True) as messages:
        yield messages
    assert tuple(message.category for message in messages) == categories

warnings.simplefilter('default', DeprecationWarning)


def fixture(cls):
    return pytest.fixture(lambda: cls())


@pytest.yield_fixture
def tempdir():
    tempdir = tempfile.mkdtemp(dir=os.path.dirname(__file__))
    yield tempdir
    shutil.rmtree(tempdir)


@fixture
class constitution(object):
    fields = dict.fromkeys(['article', 'amendment', 'date'], {'stored': True, 'tokenized': False})
    fields['text'] = {'storeTermVectors': True, 'storeTermVectorPositions': True, 'storeTermVectorOffsets': True}

    def __iter__(self):
        lines = open(os.path.join(os.path.dirname(__file__), 'constitution.txt'))
        items = itertools.groupby(lines, lambda l: l.startswith('Article ') or l.startswith('Amendment '))
        for flag, (header,) in items:
            flag, lines = next(items)
            header, num = header.rstrip('.\n').split(None, 1)
            fields = {header.lower(): num, 'text': ''.join(lines)}
            if header == 'Amendment':
                num, date = num.split()
                date = datetime.strptime(date, '%m/%d/%Y').date()
                fields.update({header.lower(): num, 'date': str(date)})
            yield fields


@fixture
class zipcodes(object):
    fields = dict.fromkeys(['city', 'county', 'state', 'latitude', 'longitude'], {'stored': True, 'indexed': False})
    fields['zipcode'] = {'stored': True, 'tokenized': False}

    def __iter__(self):
        lines = open(os.path.join(os.path.dirname(__file__), 'zipcodes.txt'))
        for zipcode, latitude, longitude, state, city, county in csv.reader(lines):
            yield {
                'zipcode': zipcode,
                'latitude': float(latitude),
                'longitude': float(longitude),
                'city': city.title(),
                'county': county.title(),
                'state': state,
            }
