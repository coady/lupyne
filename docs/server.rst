server
======
.. automodule:: server

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
  --autorefresh=SECONDS
                        automatically refresh index

tools
-----------
.. autofunction:: json_tool
.. autofunction:: allow_tool

WebSearcher
-----------
.. autoclass:: server.WebSearcher
  :members:
  :exclude-members: parse

WebIndexer
-----------
.. autoclass:: server.WebIndexer
  :show-inheritance:
  :members:

start
-----------
.. autofunction:: start
