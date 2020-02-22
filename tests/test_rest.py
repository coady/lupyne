import pytest
from starlette.testclient import TestClient


@pytest.fixture
def client(index):
    from lupyne.server.rest import app

    return TestClient(app)


def test_index(client):
    response = client.get("/")
    assert response.status_code == 200
    ((directory, count),) = response.json().items()
    assert 'Directory@' in directory and count == 35
