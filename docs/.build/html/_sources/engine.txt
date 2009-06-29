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

  .. automethod:: __len__

  .. automethod:: __contains__

  .. automethod:: __iter__

  .. automethod:: __getitem__

  .. automethod:: __delitem__

    Acquires a write lock.  Deleting from an `IndexWriter`_ is encouraged instead.

Searcher
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.Searcher
  :members:

  .. automethod:: __getitem__

    Return `Document`_

  .. automethod:: __del__

    Closes index.

IndexSearcher
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexSearcher
  :show-inheritance:
  :members:

  .. attribute:: filters

    Mapping of cached filters, which are also used for facet counts.

MultiSearcher
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.MultiSearcher
  :show-inheritance:

ParallelMultiSearcher
^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: engine.indexers.ParallelMultiSearcher
  :show-inheritance:

IndexWriter
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexWriter
  :show-inheritance:
  :members:

  .. attribute:: fields

    Mapping of assigned fields.  May be used directly, instead of :meth:`set` method, for further customization.

  .. automethod:: __del__

    Closes index.

  .. automethod:: __len__

  .. automethod:: __iadd__

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

  .. automethod:: __len__

  .. automethod:: __contains__

  .. automethod:: __iter__

  .. automethod:: __getitem__

  .. automethod:: __delitem__

Hit
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Hit
  :members:

Hits
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Hits
  :members:

  .. automethod:: __len__

  .. automethod:: __getitem__

Field
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Field
  :members:

FormatField
^^^^^^^^^^^^^
.. autoclass:: engine.documents.FormatField
  :show-inheritance:
  :members:

  .. method:: format(value)

    Return formatted value.

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

  .. automethod:: __and__

    <BooleanQuery +self +other>

  .. automethod:: __or__

    <BooleanQuery self other>

  .. automethod:: __sub__

    <BooleanQuery self -other>

BooleanQuery
^^^^^^^^^^^^^
.. autoclass:: engine.queries.BooleanQuery
  :members:

  .. automethod:: __len__

  .. automethod:: __iter__

  .. automethod:: __getitem__

  .. automethod:: __iand__

    add +other

  .. automethod:: __ior__

    add other

  .. automethod:: __isub__

    add -other

SpanQuery
^^^^^^^^^^^^^
.. autoclass:: engine.queries.SpanQuery
  :members:

  .. automethod:: __getitem__

    <SpanFirstQuery: spanFirst(self, other.stop)>

  .. automethod:: __sub__

    <SpanNotQuery: spanNot(self, other)>

  .. automethod:: __or__

    <SpanOrQuery: spanOr(spans)>

Filter
^^^^^^^^^^^^^
.. autoclass:: engine.queries.Filter
  :members:


spatial
---------
.. automodule:: engine.spatial

Tiler
^^^^^^^^^^^^^
.. autoclass:: engine.spatial.Tiler
  :members:

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
