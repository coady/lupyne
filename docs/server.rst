server
======
.. program-output:: python -m lupyne.server -h
  :cwd: ..
.. automodule:: lupyne.server
.. note:: Lucene doc ids are ephemeral;  only use doc ids across requests for the same index version.

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

.. _CherryPy tools: http://docs.cherrypy.org/stable/progguide/extending/customtools.html
