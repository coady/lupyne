client
======
.. automodule:: lupyne.client

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
  :undoc-members:

  .. automethod:: getresponse

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
