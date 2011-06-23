server
======
Usage: python server.py [index_directory ...]

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
  --autoupdate=SECONDS  automatically update index version

.. automodule:: server

tools
-----------
`CherryPy tools <http://www.cherrypy.org/wiki/CustomTools>`_ enabled by default:  tools.\{json,allow,time,validate\}.on

.. autofunction:: json_
.. autofunction:: allow
.. autofunction:: time_
.. autofunction:: validate

WebSearcher
-----------
.. autoclass:: server.WebSearcher
  :members:
  :exclude-members: new

WebIndexer
-----------
.. autoclass:: server.WebIndexer
  :show-inheritance:
  :members:

start
-----------
.. autofunction:: mount
.. autofunction:: start
