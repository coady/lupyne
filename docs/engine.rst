engine
======
.. automodule:: engine


indexers
---------
.. automodule:: engine.indexers

IndexReader
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexReader
  :members:

  Provides a mapping interface of ids to document objects.

  .. method:: __len__()

  .. method:: __contains__(id)

  .. method:: __iter__()

  .. method:: __getitem__(id)

  .. method:: __delitem__(id)

    Acquires a write lock.  Deleting from an `IndexWriter`_ is encouraged instead.


IndexSearcher
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexSearcher
  :show-inheritance:
  :members:

  Provides a mapping interface of ids to document objects.

  .. attribute:: filters

    Mapping of cached filters, which are also used for facet counts.

  .. method:: __del__()

    Closes index.


IndexWriter
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexWriter
  :show-inheritance:
  :members:

  .. method:: __del__()

    Closes index.

  .. method:: __len__()

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

  .. method:: __len__()

  .. method:: __contains__(name)

  .. method:: __iter__()

  .. method:: __getitem__(name)

  .. method:: __delitem__(name)

Hit
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Hit
  :members:

Hits
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Hits
  :members:

  .. method:: __len__()

  .. method:: __getitem__(index)

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

DateTimeField
^^^^^^^^^^^^^
.. autoclass:: engine.documents.DateTimeField
  :show-inheritance:
  :members:


queries
---------
.. automodule:: engine.queries

Query
^^^^^^^^^^^^^
.. autoclass:: engine.queries.Query
  :members:

  .. method:: __and__(self, other)

    <BooleanQuery +self +other>

  .. method:: __or__(self, other)

    <BooleanQuery self other>

  .. method:: __sub__(self, other)

    <BooleanQuery self -other>

BooleanQuery
^^^^^^^^^^^^^
.. autoclass:: engine.queries.BooleanQuery
  :members:

  .. method:: __len__()

  .. method:: __iter__()

  .. method:: __getitem__(index)

  .. method:: __iand__(self, other)

    add +other

  .. method:: __ior__(self, other)

    add other

  .. method:: __isub__(self, other)

    add -other

SpanQuery
^^^^^^^^^^^^^
.. autoclass:: engine.queries.SpanQuery
  :members:

  .. method:: __getitem__(self, other)

    <SpanFirstQuery: spanFirst(self, other.stop)>

  .. method:: __sub__(self, other)

    <SpanNotQuery: spanNot(self, other)>

  .. method:: __or__(*spans)

    <SpanOrQuery: spanOr(spans)>

Filter
^^^^^^^^^^^^^
.. autoclass:: engine.queries.Filter
  :members:


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
