import lucene

lucene.initVM(vmargs='-Djava.awt.headless=true')


def pytest_report_header(config):
    return 'PyLucene ' + lucene.VERSION
