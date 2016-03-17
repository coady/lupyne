import lucene
from .fixtures import tempdir, constitution, zipcodes  # noqa


def pytest_report_header(config):
    return 'PyLucene ' + lucene.VERSION


def pytest_configure(config):
    assert lucene.initVM(vmargs='-Djava.awt.headless=true')
