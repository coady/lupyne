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
 * Optimized faceted and grouped search.
 * Optimized prefix and range queries.
 * Geospatial support.
 * Spellchecking.

Changes in 0.7:
==================
 * Support for Lucene 2.4 dropped
 * CherryPy 3.2 compatibile
 * Spatial within queries optimized and allow unlimited distance
 * Searches can be timed out
 * Sorted searches allow computing scores and tracking maxscore
 * Disjunction queries
 * Numeric range queries with custom precision step
 * Enumeration of numeric terms
 * Efficient copying of a subset of indexes
 * Loading searchers into a RAMDirectory
 * SortFields support custom parsers and field cache reuse
 * Server:
   
   - response time returned in headers
   - multiple sort keys with unlimited search results
   - return maxscore with support for time outs and sorting
   - filter search results, with caching
   - json responses configurable for pretty printing
   - vm initialization compatible with daemonizing
   - support for mounting mutltiple root websearchers
   - autorefresh of server data can be customized
   - documents return cached indexed fields
   - grouping document results
   - optional query parsing to support unanalyzed fields
   - refreshing websearchers has better cache support
 * Client:
   
   - reuses connection in the event of a timeout
   - pipelining requests
   - raises deprecation warnings from server
   - stores response time from server
"""

import os
from distutils.core import setup

packages = []
for dirpath, dirnames, filenames in os.walk('lupyne'):
    dirnames[:] = [dirname for dirname in dirnames if not dirname.startswith('.')]
    packages.append(dirpath.replace(os.sep, '.'))

setup(
    name='lupyne',
    version='0.7',
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
