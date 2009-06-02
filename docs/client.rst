client
======
.. automodule:: client


Response
---------
.. autoclass:: client.Response
  :show-inheritance:
  :members:

  .. method:: __call__()
  
  Return evaluated response body or raise exception.

Resource
---------
.. autoclass:: client.Resource
  :show-inheritance:
  :members:
  :undoc-members:

  .. method:: __call__(method, path, body=None)

  Send request and return evaluated response body.

Resources
---------
.. autoclass:: client.Resources
  :members:
