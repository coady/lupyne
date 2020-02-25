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
