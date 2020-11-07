Services use [Starlette's config](https://www.starlette.io/config/): in environment variables or a .env file

## [GraphQL](https://graphql.org)
```console
% DIRECTORIES=... SCHEMA=... uvicorn lupyne.services.graphql:app
```

Open <http://localhost:8000/graphql>.

Creating a graphql schema is strongly recommended, to retrieve stored fields and sort by fields.

```graphql
"""stored fields"""
type Document {
  name: String
  tag: [String!]!
  size: Int
}

"""sort fields"""
type FieldDoc {
  date: String
}
```

## [REST](https://en.wikipedia.org/wiki/Representational_state_transfer)
```console
% DIRECTORIES=... SCHEMA=... uvicorn lupyne.services.rest:app
```

Open <http://localhost:8000/docs>.

Creating a graphql schema is also recommended - even though it's REST - to retrieve stored fields and sort by fields.
