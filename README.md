[![image](https://img.shields.io/pypi/v/lupyne.svg)](https://pypi.org/project/lupyne/)
![image](https://img.shields.io/pypi/pyversions/lupyne.svg)
[![image](https://pepy.tech/badge/lupyne)](https://pepy.tech/project/lupyne)
![image](https://img.shields.io/pypi/status/lupyne.svg)
[![build](https://github.com/coady/lupyne/actions/workflows/build.yml/badge.svg)](https://github.com/coady/lupyne/actions/workflows/build.yml)
[![image](https://codecov.io/gh/coady/lupyne/branch/main/graph/badge.svg)](https://codecov.io/gh/coady/lupyne/)
[![CodeQL](https://github.com/coady/lupyne/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/coady/lupyne/actions/workflows/github-code-scanning/codeql)
[![image](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![image](https://mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

Lupyne is a search engine based on [PyLucene](https://lucene.apache.org/pylucene/), the Python extension for accessing Java Lucene. Lucene is a relatively low-level toolkit, and PyLucene wraps it through automatic code generation. So although Java idioms are translated to Python idioms where possible, the resulting interface is far from Pythonic. See `./docs/examples.ipynb` for comparisons with the Lucene API.

Lupyne also provides GraphQL and RESTful search services, based on [Starlette](https://www.starlette.io). Note Solr and Elasticsearch are popular options for Lucene-based search, if no further (Python) customization is needed. So while the services are suitable for production usage, their primary motivation is to be an extensible example.

Not having to initially choose between an embedded library and a server not only provides greater flexibility, it can provide better performance, e.g., batch indexing offline and remote searching live. Additionally only lightweight wrappers with extended behavior are used wherever possible, so falling back to using PyLucene directly is always an option, but should never be necessary for performance.

## Usage
PyLucene requires initializing the VM.

```python
import lucene

lucene.initVM()
```

Indexes are accessed through an `IndexSearcher` (read-only), `IndexWriter`, or the combined `Indexer`.

```python
from lupyne import engine

searcher = engine.IndexSearcher('index/path')
hits = searcher.search('text:query')
```

See `./lupyne/services/README.md` for services usage.

## Installation
```console
% pip install lupyne[graphql,rest]
```

PyLucene is not `pip` installable.
* [Install instructions](https://lucene.apache.org/pylucene/install.html)
* [Docker](https://hub.docker.com) image: `docker pull coady/pylucene`
* [Homebrew](https://brew.sh) formula: `brew install coady/tap/pylucene`

## Dependencies
* PyLucene >=9.6
* strawberry-graphql (if graphql option)
* fastapi (if rest option)

## Tests
100% branch coverage.

```console
% pytest [--cov]
```
