.. Lupyne documentation master file, created by
   sphinx-quickstart on Wed Sep 28 15:41:40 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Lupyne's documentation
==================================

Lupyne_ is:
 * a high-level Pythonic search `engine <engine.html>`_ library, built on PyLucene_
 * a RESTful_ JSON_ search `server <server.html>`_, built on CherryPy_
 * a simple Python `client <client.html>`_ for interacting with the server

Quickstart
^^^^^^^^^^

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

Contents
^^^^^^^^^^

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

.. _Lupyne: https://bitbucket.org/coady/lupyne
.. _PyLucene: http://lucene.apache.org/pylucene/
.. _RESTful: http://en.wikipedia.org/wiki/Representational_State_Transfer
.. _JSON: http://json.org/
.. _CherryPy: http://cherrypy.org
