server
======
.. program-output:: python3 -m lupyne.server -h
  :cwd: ..
.. automodule:: lupyne.server.legacy
.. note:: Lucene doc ids are ephemeral;  only use doc ids across requests for the same index version.

Lucene index files are incremental, so synchronizing files and refreshing searchers is a viable replication strategy.
Both searchers and indexers support `autoupdate`, and indexers support snapshots,
which allow replicating index files safely and atomically.

CherryPy was chosen because not only is it well suited to exposing APIs, but it includes a production multithreaded server.
Lucene caches heavily, and PyLucene is not bound by the `GIL`_ when in the Java VM.
So threads are a natural choice for a worker pool, even if a different concurrency model is used for HTTP.

tools
-----------
`CherryPy tools`_ enabled by default:  tools.\{json_in,json_out,allow,timer,validate\}.on

.. autofunction:: json_in
.. autofunction:: json_out
.. autofunction:: allow
.. autofunction:: timer
.. autofunction:: validate

WebSearcher
-----------
.. autoclass:: WebSearcher
  :members:

  .. versionchanged:: 2.5
    automatic synchronization and promotion removed

WebIndexer
-----------
.. autoclass:: WebIndexer
  :show-inheritance:
  :members:

start
-----------
.. autofunction:: start

.. _CherryPy tools: http://docs.cherrypy.org/en/latest/extend.html#tools
.. _GIL: https://docs.python.org/3/glossary.html#term-gil