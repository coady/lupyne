About Lupyne
==================
 * high-level Pythonic search engine interface to PyLucene_
 * RESTful JSON CherryPy_ server
 * simple Python client for interacting with the server

Installation
==================
Standard installation from local download or directly from pypi.
::

  python setup.py install
  pip install lupyne

Dependencies
==================
Lupyne should run anywhere PyLucene does, though its primary testing is on the popular unix variants.

 * Python 2.6.6+, 2.7
 * PyLucene 3.5, 3.6, 4.3, 4.4
 * CherryPy 3.2 (only required for server)

Usage
==================
See examples and documentation.  Sphinx required to build docs.
::

  ./examples
  ./docs/README.html

Tests
==================
Run full coverage tests.
::

  python -m test
  python2.6 -m test.__main__

.. _PyLucene: http://lucene.apache.org/pylucene/
.. _CherryPy: http://cherrypy.org
