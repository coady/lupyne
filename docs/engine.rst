engine
======
.. automodule:: engine


indexers
---------
.. automodule:: engine.indexers

IndexSearcher
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexSearcher
  :show-inheritance:
  :members:
  :inherited-members:

  Provides a mapping interface of ids to document objects.

  .. method:: __del__():
  
    Closes index.

  .. method:: __len__():

  .. method:: __contains__(id):

  .. method:: __iter__():

  .. method:: __getitem__(id):

  .. method:: __delitem__(id):
  
    Acquires a write lock.  Deleting from an `IndexWriter`_ is encouraged instead.


IndexWriter
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexWriter
  :show-inheritance:
  :members:

  .. method:: __del__():
  
    Closes index.

  .. method:: __len__():

  .. method:: __iadd__(directory)
  
    Add directory (or reader, searcher, writer) to index.

Indexer
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.Indexer
  :show-inheritance:
  :members:


documents
---------
.. automodule:: engine.documents

Document
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Document
  :members:

  .. method:: __len__():

  .. method:: __contains__(name):

  .. method:: __iter__():

  .. method:: __getitem__(name):

  .. method:: __delitem__(name):

Hit
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Hit
  :members:

Hits
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Hits
  :members:

  .. method:: __len__():

  .. method:: __contains__(name):

Field
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Field
  :members:

PrefixField
^^^^^^^^^^^^^
.. autoclass:: engine.documents.PrefixField
  :show-inheritance:
  :members:

NestedField
^^^^^^^^^^^^^
.. autoclass:: engine.documents.NestedField
  :show-inheritance:
  :members:


queries
---------
.. automodule:: engine.queries

Query
^^^^^^^^^^^^^
.. autoclass:: engine.queries.Query

  .. automethod:: term(name, value)

  .. automethod:: prefix(name, value)

  .. automethod:: range(self, name, lower, upper, inclusive=False)

  .. method:: __and__(self, other):
  
    self AND other

  .. method:: __or__(self, other):
  
    self OR other

  .. method:: __sub__(self, other):
  
    self NOT other

spatial
---------
.. automodule:: engine.spatial

PointField
^^^^^^^^^^^^^
.. autoclass:: engine.spatial.PointField
  :show-inheritance:
  :members:

PolygonField
^^^^^^^^^^^^^
.. autoclass:: engine.spatial.PolygonField
  :show-inheritance:
  :members:
