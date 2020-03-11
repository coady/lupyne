import pytest
from starlette.testclient import TestClient


@pytest.fixture
def client(index):
    from lupyne.server.rest import app

    return TestClient(app)


def test_index(client):
    result = client.get('/').json()
    ((directory, count),) = result.items()
    assert 'Directory@' in directory
    assert count == 35
    resp = client.post('/')
    assert resp.json() == result
    assert float(resp.headers['x-response-time']) > 0.0
    assert int(resp.headers['age']) == 0
    assert client.post('/', params={'spellcheckers': True}).ok


def test_terms(client):
    result = client.get('/terms').json()
    assert result == ['amendment', 'article', 'date', 'text']
    result = client.get('/terms/date').json()
    assert min(result) == result[0] == '1791-12-15'
    result = client.get('/terms/date', params={'counts': True}).json()
    assert result['1791-12-15'] == 10


def test_search(client):
    result = client.get('/search', params={'q': "text:right", 'count': 1}).json()
    assert result['count'] == 13
    (hit,) = result['hits']
    assert hit['id'] == 9
    assert hit['score'] > 0
    assert hit['sortkeys'] == []
    assert hit['doc'] == {'amendment': ['2'], 'date': ['1791-12-15']}
