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
