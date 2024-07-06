import os
import pytest
from starlette import testclient
from .conftest import fixtures


class TestClient(testclient.TestClient):
    def execute(self, query, **variables):
        response = self.post('/graphql', json={'query': query, 'variables': variables})
        response.raise_for_status()
        result = response.json()
        for error in result.get('errors', []):
            raise ValueError(error)
        return result['data']


@pytest.fixture
def client(index):
    os.environ['SCHEMA'] = str(fixtures / 'constitution.graphql')
    from lupyne.services.graphql import app

    return TestClient(app)


def test_index(client):
    data = client.execute('{ index { directories counts } }')
    index = data['index']
    (directory,) = index['directories']
    assert 'Directory@' in directory
    assert index['counts'] == [35]
    data = client.execute('mutation { index { directories counts } }')
    assert data == {'index': index}


def test_terms(client):
    data = client.execute('{ terms { date { values } } }')
    dates = data['terms']['date']['values']
    assert min(dates) == dates[0] == '1791-12-15'
    data = client.execute('{ terms { date { counts } } }')
    counts = data['terms']['date']['counts']
    assert counts[0] == 10


def test_search(client):
    data = client.execute(
        '{ search(q: "text:right", count: 1) { count hits { id score sortkeys { year } doc { amendment } } } }'
    )
    assert data['search']['count'] == 13
    (hit,) = data['search']['hits']
    assert hit['id'] == 9
    assert hit['score'] > 0
    assert hit['sortkeys'] == {'year': None}
    assert hit['doc'] == {'amendment': '2'}
    data = client.execute("""{ search(q: "text:right", count: 1, sort: ["-year"])
        { count hits { id score sortkeys { year } doc { amendment } } } }""")
    assert data['search']['count'] == 13
    (hit,) = data['search']['hits']
    assert hit == {
        'id': 33,
        'score': pytest.approx(0.648349),
        'sortkeys': {'year': 1971},
        'doc': {'amendment': '26'},
    }


def test_count(client):
    data = client.execute('{ search(q: "text:right") { count hits { id } } }')
    assert data['search']['count'] == len(data['search']['hits']) == 13
    data = client.execute('{ search(q: "text:right", count: 0) { count hits { id } } }')
    assert data['search'] == {'count': 13, 'hits': []}
    data = client.execute('{ search(q: "text:right") { count } }')
    assert data['search'] == {'count': 13}
