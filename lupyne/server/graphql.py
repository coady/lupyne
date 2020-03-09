from typing import List
import lucene
import strawberry.asgi
from starlette.applications import Starlette
from .settings import DEBUG, DIRECTORIES
from .base import WebSearcher

assert lucene.getVMEnv() or lucene.initVM()
root = WebSearcher(*DIRECTORIES)
app = Starlette(debug=DEBUG)
app.on_event('shutdown')(root.close)


@strawberry.type
class Index:
    directories: List[str]
    counts: List[int]


@strawberry.type
class Query:
    @strawberry.field
    def index(self, info) -> Index:
        """Return index information."""
        index = root.index()
        return Index(directories=index, counts=index.values())


@strawberry.type
class Mutation:
    @strawberry.field
    def index(self, info, spellcheckers: bool = False) -> Index:
        """Refresh index."""
        index = root.refresh(spellcheckers=spellcheckers)
        return Index(directories=index, counts=index.values())


schema = strawberry.Schema(query=Query, mutation=Mutation)
app.add_route('/graphql', strawberry.asgi.GraphQL(schema, debug=DEBUG))
