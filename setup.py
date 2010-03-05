"""
Project highlights
==================
The core engine is a high level interface to PyLucene, which is a Python extension for accessing the popular Java Lucene search engine.
Lucene has a reputation for being a relatively low-level toolkit, and the goal of PyLucene is to wrap it through automatic code generation.
So although PyLucene transforms Java idioms to Python idioms where possible, the resulting interface is far from Pythonic.
See examples for comparisons with the Lucene API.

A RESTful JSON search server, based on CherryPy.
Many python applications which require better search capabilities are migrating from using conventional client-server databases, whereas Lucene is an embedded search library.
Solr is a popular option for remote searching and other advanced features, but then any customization or embedded use is coupled to Java and XML.
Using a python web framework instead can provide the best of both worlds, e.g., batch indexing offline and remote searching live.

A simple client to make interacting with the server as convenient as an RPC interface.
It handles all of the HTTP interactions, with support for compression, json, and connection reuse.

Advanced search features:
 * Distributed searching with support for redundancy, partitioning, and sharding.
 * Optimized faceted search.
 * Optimized prefix and range queries.
 * Geospatial support.

Changes in 0.5:
==================
 * PyLucene 2.4 support is deprecated
 * Optimized facets
 * MultiSearchers support full searching interface
 * Reopening searchers with resource closing managed
 * Query parsing supports boost mapping
 * Remote searching support:
 
   - production configuration
   - refreshing searcher
   - "more like this" queries
   - retrieving document slices
   - hit text highlighting
   - query parsing options
   - json errors
   - advanced term enumeration
   - multisearchers
   - sorting options
   - facets
"""

import os
from distutils.core import setup

packages = []
for dirpath, dirnames, filenames in os.walk('lupyne'):
    dirnames[:] = [dirname for dirname in dirnames if not dirname.startswith('.')]
    packages.append(dirpath.replace(os.sep, '.'))

setup(
    name='lupyne',
    version='0.5+',
    description='Pythonic search engine based on PyLucene, including a standalone server based on CherryPy.',
    long_description=__doc__,
    author='Aric Coady',
    author_email='aric.coady@gmail.com',
    url='http://code.google.com/p/lupyne/',
    packages=packages,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
    ],
)
