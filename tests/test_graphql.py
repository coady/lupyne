import pytest
from starlette import testclient


class TestClient(testclient.TestClient):
    def execute(self, **kwargs):
        response = self.post('/graphql', json=kwargs)
        response.raise_for_status()
        result = response.json()
        for error in result.get('errors', []):
            raise ValueError(error)
        return result['data']


@pytest.fixture
def client(index):
    from lupyne.server.graphql import app

    return TestClient(app)


def test_index(client):
    data = client.execute(query='''{ index { directories counts } }''')
    index = data['index']
    (directory,) = index['directories']
    assert 'Directory@' in directory
    assert index['counts'] == [35]
    data = client.execute(query='''mutation { index { directories } }''')
    assert data == {'index': {'directories': [directory]}}
    data = client.execute(query='''mutation { index(spellcheckers: true) { counts } }''')
    assert data == {'index': {'counts': index['counts']}}


def test_terms(client):
    data = client.execute(query='''{ terms { values { date } } }''')
    dates = data['terms']['values']['date']
    assert min(dates) == dates[0] == '1791-12-15'
    data = client.execute(query='''{ terms { counts { date } } }''')
    counts = data['terms']['counts']['date']
    assert counts[0] == 10
