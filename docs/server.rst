server
======
Usage: python -m lupyne.server [index_directory ...]

Options:
  -h, --help            show this help message and exit
  -r, --read-only       expose only read methods; no write lock
  -c CONFIG, --config=CONFIG
                        optional configuration file or json object of global
                        params
  -p FILE, --pidfile=FILE
                        store the process id in the given file
  -d, --daemonize       run the server as a daemon
  --autoreload=SECONDS  automatically reload modules; replacement for
                        engine.autoreload
  --autoupdate=SECONDS  automatically update index version and commit any
                        changes
  --autosync=HOSTS
                        automatically synchronize searcher with remote hosts
                        and update
  --real-time           search in real-time without committing

.. automodule:: lupyne.server
.. note::
  Lucene doc ids are ephemeral;  only use doc ids across requests for the same index version.

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
