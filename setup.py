"""
*Pythonic extensions to PyLucene, including a standalone search server based on CherryPy.*

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

Changes in 0.4:
==================
 * PyLucene 3.0 support; deprecated calls have been dropped where possible.
 * Alternative numeric implementations of SpatialFields and DateTimeFields.
 * Custom TokenFilters, with support for generators.
 * Expanded custom Analyzers and parsing.
 * Term enumerations support wildcards and fuzziness.
 * IndexReaders support MoreLikeThis queries.
 * Index writers and searchers support managing their analyzers and directories.
 * Optimized and more versatile sorting and comparators.
"""

import os
from distutils.core import setup

packages = []
for dirpath, dirnames, filenames in os.walk('lupyne'):
    dirnames[:] = [dirname for dirname in dirnames if not dirname.startswith('.')]
    packages.append(dirpath.replace(os.sep, '.'))

setup(
    name='lupyne',
    version='0.4+',
    description='A pythonic search engine, based on PyLucene and CherryPy.',
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
