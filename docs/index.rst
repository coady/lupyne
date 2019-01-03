.. Lupyne documentation master file, created by sphinx-quickstart.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Lupyne's documentation
==================================

Lupyne_ is:
 * a high-level Pythonic search `engine <engine.html>`_ library, built on PyLucene_
 * a RESTful_ JSON_ search `server <server.html>`_, built on CherryPy_

Quickstart
^^^^^^^^^^

  >>> from lupyne import engine                       # don't forget to call lucene.initVM
  >>> indexer = engine.Indexer()                      # create an in-memory index (no filename supplied)
  >>> indexer.set('name', stored=True)                # create stored 'name' field
  >>> indexer.set('text', engine.Field.Text)          # create indexed 'text' field
  >>> indexer.add(name='sample', text='hello world')  # add a document to the index
  >>> indexer.commit()                                # commit changes; document is now searchable
  >>> hits = indexer.search('text:hello')             # run search and return sequence of documents
  >>> len(hits), hits.count                           # 1 hit retrieved (out of a total of 1)
  (1, 1)
  >>> hit, = hits
  >>> hit['name']                                     # hits support mapping interface for their stored fields
  'sample'
  >>> hit.id, hit.score                               # plus internal doc number and score
  (0, 0.28768208622932434)
  >>> hit.dict()                                      # dict representation of the hit document
  {'name': 'sample', '__id__': 0, '__score__': 0.28768208622932434}

See more `examples <examples.html>`_

Contents
^^^^^^^^^^

.. toctree::
   :maxdepth: 3

   engine
   server

.. toctree::
   :hidden:

   examples

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Lupyne: https://pypi.org/project/lupyne/
.. _PyLucene: http://lucene.apache.org/pylucene/
.. _RESTful: http://en.wikipedia.org/wiki/Representational_State_Transfer
.. _JSON: http://json.org/
.. _CherryPy: http://cherrypy.org
