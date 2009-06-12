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

  .. method:: __call__()

    Return json evaluated response body or raise exception.

Resource
---------
.. autoclass:: client.Resource
  :show-inheritance:
  :members:
  :undoc-members:

  .. method:: __call__(method, path, body=None)

    Send request and return evaluated response body.

  .. method:: getresponse()

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
