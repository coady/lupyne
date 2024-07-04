import csv
import itertools
import os
import shutil
import sys
import tempfile
from datetime import datetime
from importlib import metadata
from pathlib import Path
import pytest
import lucene
from lupyne import engine

fixtures = Path(__file__).parent / 'fixtures'


def pytest_report_header(config):
    return 'lucene: ' + metadata.version('lucene')


def pytest_configure(config):
    assert lucene.initVM()


@pytest.fixture
def tempdir():
    tempdir = tempfile.mkdtemp(dir=fixtures)
    os.environ['DIRECTORIES'] = tempdir
    sys.modules.pop('lupyne.services.settings', None)
    yield tempdir
    shutil.rmtree(tempdir)


def fixture(gen):
    return pytest.fixture(lambda: gen())


@pytest.fixture
def fields():
    return [
        engine.Field.Text(
            'text',
            storeTermVectors=True,
            storeTermVectorPositions=True,
            storeTermVectorOffsets=True,
        ),
        engine.Field.String('article', stored=True),
        engine.Field.String('amendment', stored=True),
        engine.Field.String('date', stored=True, docValuesType='sorted'),
        engine.Field('year', docValuesType='numeric'),
    ]


@fixture
def constitution():
    lines = open(fixtures / 'constitution.txt')
    key = lambda l: l.startswith('Article ') or l.startswith('Amendment ')  # noqa
    items = itertools.groupby(lines, key)
    for _, (header,) in items:
        _, lines = next(items)
        header, num = header.rstrip('.\n').split(None, 1)
        fields = {header.lower(): num, 'text': ''.join(lines)}
        if header == 'Amendment':
            num, date = num.split()
            date = datetime.strptime(date, '%m/%d/%Y').date()
            fields |= {header.lower(): num, 'date': str(date), 'year': date.year}
        yield fields


@fixture
def zipcodes():
    lines = open(fixtures / 'zipcodes.txt')
    for zipcode, latitude, longitude, state, city, county in csv.reader(lines):
        yield {
            'zipcode': zipcode,
            'latitude': float(latitude),
            'longitude': float(longitude),
            'city': city.title(),
            'county': county.title(),
            'state': state,
        }


@pytest.fixture
def index(tempdir, fields, constitution):
    with engine.IndexWriter(tempdir) as writer:
        writer.fields |= {field.name: field for field in fields}
        for doc in constitution:
            writer.add(doc)
    return tempdir
