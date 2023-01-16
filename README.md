[![image](https://img.shields.io/pypi/v/lupyne.svg)](https://pypi.org/project/lupyne/)
![image](https://img.shields.io/pypi/pyversions/lupyne.svg)
[![image](https://pepy.tech/badge/lupyne)](https://pepy.tech/project/lupyne)
![image](https://img.shields.io/pypi/status/lupyne.svg)
[![image](https://github.com/coady/lupyne/workflows/build/badge.svg)](https://github.com/coady/lupyne/actions)
[![image](https://codecov.io/gh/coady/lupyne/branch/main/graph/badge.svg)](https://codecov.io/gh/coady/lupyne/)
[![image](https://github.com/coady/lupyne/workflows/codeql/badge.svg)](https://github.com/coady/lupyne/security/code-scanning)
[![image](https://img.shields.io/badge/code%20style-black-000000.svg)](https://pypi.org/project/black/)
[![image](http://mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

Lupyne is a search engine based on [PyLucene](http://lucene.apache.org/pylucene/), the Python extension for accessing Java Lucene. Lucene is a relatively low-level toolkit, and PyLucene wraps it through automatic code generation. So although Java idioms are translated to Python idioms where possible, the resulting interface is far from Pythonic. See `./docs/examples.ipynb` for comparisons with the Lucene API.

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
* [Install instructions](http://lucene.apache.org/pylucene/install.html)
* [Docker](https://hub.docker.com) image: `docker pull coady/pylucene`
* [Homebrew](https://brew.sh) formula: `brew install coady/tap/pylucene`

## Dependencies
* PyLucene >=9.1
* strawberry-graphql >=0.84.4 (if graphql option)
* fastapi (if rest option)

## Tests
100% branch coverage.

```console
% pytest [--cov]
```

## Changes
3.0

* PyLucene >=9.1 required
* [CherryPy](https://cherrypy.org) server removed

2.5

* Python >=3.7 required
* PyLucene 8.6 supported
* [CherryPy](https://cherrypy.org) server deprecated

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
