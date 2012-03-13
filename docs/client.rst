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
  :exclude-members: response_class

Pool
---------
.. autoclass:: Pool
  :show-inheritance:
  :members:

Resources
---------
.. autoclass:: Resources
  :show-inheritance:
  :members:
  :exclude-members: queue

Shards
---------
.. autoclass:: Shards
  :show-inheritance:
  :members:

  .. attribute:: resources

    `Resources`_ mapping.

Replicas
---------
.. autoclass:: Replicas
  :show-inheritance:
  :members:
