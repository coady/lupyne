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

tools
-----------
`CherryPy tools <http://docs.cherrypy.org/stable/progguide/extending/customtools.html>`_ enabled by default:  tools.\{json,allow,time,validate\}.on

.. autofunction:: json_
.. autofunction:: allow
.. autofunction:: time_
.. autofunction:: validate

WebSearcher
-----------
.. autoclass:: WebSearcher
  :members:
  :exclude-members: new, sync

WebIndexer
-----------
.. autoclass:: WebIndexer
  :show-inheritance:
  :members:

start
-----------
.. autofunction:: mount
.. autofunction:: start
