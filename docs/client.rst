client
======
.. automodule:: client

Response
---------
.. autoclass:: client.Response
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
.. autoclass:: client.Resource
  :show-inheritance:
  :members:
  :undoc-members:

  .. automethod:: getresponse

Resources
---------
.. autoclass:: client.Resources
  :show-inheritance:
  :members:

Shards
---------
.. autoclass:: client.Shards
  :show-inheritance:
  :members:

  .. attribute:: resources

    `Resources`_ mapping.
