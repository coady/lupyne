import pytest
from starlette import testclient


class TestClient(testclient.TestClient):
    def execute(self, **kwargs):
        response = self.post('/graphql', json=kwargs)
        assert response.status_code == 200
        return response.json()['data']


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
