import pytest
from starlette import testclient


class TestClient(testclient.TestClient):
    def execute(self, **kwargs):
        result = self.post('/graphql', json=kwargs).json()
        for error in result.get('errors', []):
            raise RuntimeError(error)
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
