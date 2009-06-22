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

  .. automethod:: __call__

Resource
---------
.. autoclass:: client.Resource
  :show-inheritance:
  :members:
  :undoc-members:

  .. automethod:: __call__

  .. automethod:: getresponse

Resources
---------
.. autoclass:: client.Resources
  :members:

Shards
---------
.. autoclass:: client.Shards
  :members:

  .. attribute:: resources

    `Resources`_ mapping.
