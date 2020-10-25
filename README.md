[![image](https://img.shields.io/pypi/v/lupyne.svg)](https://pypi.org/project/lupyne/)
![image](https://img.shields.io/pypi/pyversions/lupyne.svg)
[![image](https://pepy.tech/badge/lupyne)](https://pepy.tech/project/lupyne)
![image](https://img.shields.io/pypi/status/lupyne.svg)
[![image](https://github.com/coady/lupyne/workflows/build/badge.svg)](https://github.com/coady/lupyne/actions)
[![image](https://img.shields.io/codecov/c/github/coady/lupyne.svg)](https://codecov.io/github/coady/lupyne)
[![image](https://requires.io/github/coady/lupyne/requirements.svg?branch=main)](https://requires.io/github/coady/lupyne/requirements/)
[![image](https://img.shields.io/badge/code%20style-black-000000.svg)](https://pypi.org/project/black/)
[![image](http://mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

Lupyne is a search engine based on [PyLucene](http://lucene.apache.org/pylucene/), the Python extension for accessing Java Lucene.
Lucene is a relatively low-level toolkit, and PyLucene wraps it through automatic code generation.
So although Java idioms are translated to Python idioms where possible, the resulting interface is far from Pythonic.
See `./docs/examples.ipynb` for comparisons with the Lucene API.

Lupyne also provides a RESTful JSON search server, based on [CherryPy](http://cherrypy.org).
Note Solr and Elasticsearch are popular options for Lucene-based search, if no further (Python) customization is needed.
So while the server is suitable for production usage, its primary motivation is to be an extensible example.

Not having to initially choose between an embedded library and a server not only provides greater flexibility,
it can provide better performance, e.g., batch indexing offline and remote searching live.
Additionally only lightweight wrappers with extended behavior are used wherever possible,
so falling back to using PyLucene directly is always an option, but should never be necessary for performance.

# Usage
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

Run the server.

```console
% python -m lupyne.server
```

Read the [documentation](https://coady.github.io/lupyne/).

# Installation
```console
% pip install lupyne[server]
```

PyLucene is not `pip` installable.
* [Install instructions](http://lucene.apache.org/pylucene/install.html)
* [Docker](https://hub.docker.com) image: `docker pull coady/pylucene`
* [Homebrew](https://brew.sh) formula: `brew install coady/tap/pylucene`

# Dependencies
* PyLucene >=8
* cherrypy >=11 (if server option)

# Tests
100% branch coverage.

```console
% pytest [--cov]
```

# Roadmap
The original cherrypy `server` is deprecated and being replaced with [starlette](https://www.starlette.io) based services which support OpenAPI and GraphQL.

# Changes
dev
* Python >=3.7 required
* PyLucene 8.6 supported
* Simplified server command-line options

2.4
* PyLucene >=8 required
* `Hit.keys` renamed to `Hit.sortkeys`

2.3
* PyLucene >=7.7 required
* PyLucene 8 supported

2.2
* PyLucene 7.6 supported

2.1
* PyLucene >=7 required

2.0
* PyLucene >=6 required
* Python 3 support
* client moved to external package

1.9
* Python 2.6 dropped
* PyLucene 4.8 and 4.9 dropped
* IndexWriter implements context manager
* Server DocValues updated via patch method
* Spatial tile search optimized

1.8
* PyLucene 4.10 supported
* PyLucene 4.6 and 4.7 dropped
* Comparator iteration optimized
* Support for string based FieldCacheRangeFilters
