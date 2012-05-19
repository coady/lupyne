client
======
.. automodule:: lupyne.client
.. note:: Caching more client connections than the backend server supports may cause blocking under load.  CherryPy's default `thread pool`_ is 10.

Response
---------
.. autoclass:: Response
  :show-inheritance:
  :members:

  .. attribute:: status

    HTTP status code

  .. attribute:: reason

    HTTP status message

  .. attribute:: body

    string of entire response body

  .. attribute:: time

    server response time

  .. automethod:: __nonzero__

  .. automethod:: __call__

Resource
---------
.. autoclass:: Resource
  :show-inheritance:
  :members:
  :exclude-members: response_class

Pool
---------
.. versionadded:: 1.2
.. autoclass:: Pool
  :show-inheritance:
  :members:

Resources
---------
.. autoclass:: Resources
  :show-inheritance:
  :members:

Shards
---------
.. autoclass:: Shards
  :show-inheritance:
  :members:

  .. attribute:: resources

    `Resources`_ mapping.

Replicas
---------
.. versionadded:: 1.2
.. autoclass:: Replicas
  :show-inheritance:
  :members:

.. _thread pool: http://docs.cherrypy.org/stable/refman/_cpserver.html#cherrypy._cpserver.Server.thread_pool
