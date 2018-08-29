server
======
.. program-output:: python3 -m lupyne.server -h
  :cwd: ..
.. automodule:: lupyne.server
.. note:: Lucene doc ids are ephemeral;  only use doc ids across requests for the same index version.
.. warning:: Autosyncing is not recommended for production.

Lucene index files are incremental, so synchronizing files and refreshing searchers is a viable replication strategy.
The `autoupdate` and `autosync` features demonstrate this, but are not meant to recommend HTTP for file syncing.
Autoupdating is considered production-ready; autosyncing is not.

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
  :exclude-members: new, sync

  .. versionchanged:: 1.2
    automatic synchronization and promotion

  .. attribute:: fields

    optional field settings will trigger indexer promotion when synchronized hosts are exhausted

  .. attribute:: autoupdate

    optional autoupdate timer for use upon indexer promotion

WebIndexer
-----------
.. autoclass:: WebIndexer
  :show-inheritance:
  :members:

start
-----------
.. autofunction:: mount
.. autofunction:: start

.. _CherryPy tools: http://docs.cherrypy.org/en/latest/extend.html#tools
.. _GIL: https://docs.python.org/3/glossary.html#term-gil