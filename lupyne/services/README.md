# [GraphQL](https://graphql.org) service
```console
% DIRECTORIES=... uvicorn lupyne.services.graphql:app [--reload]
```

Open http://localhost:8000/graphql.

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

```console
% DIRECTORIES=... SCHEMA=... uvicorn lupyne.services.graphql:app [--reload]
```

# [REST](https://en.wikipedia.org/wiki/Representational_state_transfer) service
```console
% DIRECTORIES=... uvicorn lupyne.services.rest:app [--reload]
```

Open http://localhost:8000/docs.

Creating a graphql schema is also recommended - even though it's REST - to retrieve stored fields and sort by fields.

```console
% DIRECTORIES=... SCHEMA=... uvicorn lupyne.services.rest:app [--reload]
```
