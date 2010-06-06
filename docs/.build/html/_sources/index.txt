.. Lupyne documentation master file, created by sphinx-quickstart on Sun Jan 25 12:46:46 2009.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Lupyne's documentation!
==========================================

`Lupyne <http://code.google.com/p/lupyne/>`_ provides:
 * high-level Pythonic search `engine <engine.html>`_ interface to `PyLucene <http://lucene.apache.org/pylucene/>`_
 * `RESTful <http://en.wikipedia.org/wiki/Representational_State_Transfer>`_ `JSON <http://json.org/>`_ `CherryPy <http://cherrypy.org/>`_ `server <server.html>`_
 * simple Python `client <client.html>`_ for interacting with the server

Quickstart:

  >>> from lupyne import engine                       # don't forget to call lucene.initVM
  >>> indexer = engine.Indexer()                      # create an in-memory index (no filename supplied)
  >>> indexer.set('name', store=True)                 # create stored 'name' field
  >>> indexer.set('text')                             # create indexed 'text' field (the default)
  >>> indexer.add(name='sample', text='hello world')  # add a document to the index
  >>> indexer.commit()                                # commit changes; document is now searchable
  >>> hits = indexer.search('text:hello')             # run search and return sequence of documents
  >>> len(hits), hits.count                           # 1 hit retrieved (out of a total of 1)
  (1, 1)
  >>> hit, = hits
  >>> hit['name']                                     # hits support mapping interface for their stored fields
  u'sample'
  >>> hit.id, hit.score                               # plus internal doc number and score
  (0, 0.19178301095962524)
  >>> hit.dict()                                      # dict representation of the hit document
  {'__score__': 0.19178301095962524, u'name': u'sample', '__id__': 0}

See more `examples <examples.html>`_

Contents:

.. toctree::

   engine
   server
   client

.. toctree::
   :hidden:

   examples

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
