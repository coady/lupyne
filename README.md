[![image](https://img.shields.io/pypi/v/lupyne.svg)](https://pypi.org/project/lupyne/)
[![image](https://img.shields.io/pypi/pyversions/lupyne.svg)](https://python3statement.org)
[![image](https://pepy.tech/badge/lupyne)](https://pepy.tech/project/lupyne)
![image](https://img.shields.io/pypi/status/lupyne.svg)
[![image](https://api.shippable.com/projects/56059e3e1895ca4474182ec3/badge?branch=master)](https://app.shippable.com/github/coady/lupyne)
[![image](https://api.shippable.com/projects/56059e3e1895ca4474182ec3/coverageBadge?branch=master)](https://app.shippable.com/github/coady/lupyne)
[![image](https://requires.io/github/coady/lupyne/requirements.svg)](https://requires.io/github/coady/lupyne/requirements/)

Lupyne is a search engine based on [PyLucene](http://lucene.apache.org/pylucene/), the Python extension for accessing Java Lucene.
Lucene is a relatively low-level toolkit, and PyLucene wraps it through automatic code generation.
So although Java idioms are translated to Python idioms where possible, the resulting interface is far from Pythonic.
See `./examples` for comparisons with the Lucene API.

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

    $ python -m lupyne.server

Read the [documentation](http://lupyne.surge.sh).

# Installation

    $ pip install lupyne[server]

PyLucene is not `pip` installable.
* [Install](http://lucene.apache.org/pylucene/install.html) instructions
* [Docker](https://hub.docker.com) image: `$ docker pull coady/pylucene`
* [Homebrew](https://brew.sh) formula: `$ brew install coady/tap/pylucene`

# Dependencies
* PyLucene >=7
* six

Optional server extras:
* Python >=3.5
* cherrypy >=10
* clients >=0.2

# Tests
100% branch coverage.

    $ pytest [--cov]

# Changes
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
