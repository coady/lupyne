About Lupyne
==================
.. image:: https://img.shields.io/pypi/v/lupyne.svg
   :target: https://pypi.python.org/pypi/lupyne/
.. image:: https://img.shields.io/pypi/pyversions/lupyne.svg
.. image:: https://img.shields.io/pypi/status/lupyne.svg
.. image:: https://img.shields.io/shippable/56059e3e1895ca4474182ec3.svg
   :target: https://app.shippable.com/projects/56059e3e1895ca4474182ec3
.. image:: https://img.shields.io/codecov/c/github/coady/lupyne.svg
   :target: https://codecov.io/github/coady/lupyne

:Note: Although lupyne is maintained, its dependency `PyLucene is dormant`_ for lack of interest.

The core engine is a high level interface to `PyLucene`_, which is a Python extension for accessing the popular Java Lucene search engine.
Lucene has a reputation for being a relatively low-level toolkit, and the goal of PyLucene is to wrap it through automatic code generation.
So although PyLucene transforms Java idioms to Python idioms where possible, the resulting interface is far from Pythonic.
See ``./examples`` for comparisons with the Lucene API.

A RESTful JSON search server, based on `CherryPy`_.
Many python applications which require better search capabilities are migrating from using conventional client-server databases,
whereas Lucene is an embedded search library.  Solr and Elasticsearch are popular options for remote searching and advanced features,
but then any customization beyond the REST API is difficult and coupled to Java.
Using a python web framework instead can provide the best of both worlds, e.g., batch indexing offline and remote searching live.

A simple client to make interacting with the server as convenient as an RPC interface.
It handles all of the HTTP interactions, with support for compression, json, and connection reuse.

Advanced search features:
   * Distributed searching with support for replication, partitioning, and sharding.
   * Optimized faceted and grouped search.
   * Optimized prefix and range queries.
   * Geospatial support.
   * Spellchecking.
   * Near real-time indexing.

See `documentation`_ for example usage.

Installation
==================
Standard installation from pypi or local download. ::

   $ pip install lupyne
   $ python setup.py install

Dependencies
==================
Lupyne should run anywhere PyLucene does, though its primary testing is on the popular unix variants.

   * Python 2.7
   * PyLucene 4.10      (installed separately)
   * CherryPy 3.8+      (optional)

Tests
==================
100% branch coverage. ::

   $ pytest [--cov]

Changes
==================
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

.. _PyLucene is dormant: http://mail-archives.apache.org/mod_mbox/lucene-pylucene-dev/201506.mbox/%3calpine.OSX.2.01.1506010952020.53725@yuzu.local%3e
.. _PyLucene: http://lucene.apache.org/pylucene/
.. _CherryPy: http://cherrypy.org
.. _documentation: http://pythonhosted.org/lupyne/
