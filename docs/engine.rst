engine
======
.. automodule:: lupyne.engine

  * `TokenFilter`_, `Analyzer`_
  * `IndexSearcher`_, `MultiSearcher`_, `IndexWriter`_, `Indexer`_
  * `Document`_, `Field`_, `NestedField`_, `DateTimeField`_, `SpatialField`_
  * `Query`_


analyzers
---------
.. automodule:: lupyne.engine.analyzers

TokenStream
^^^^^^^^^^^^^
.. autoclass:: TokenStream
  :members:

TokenFilter
^^^^^^^^^^^^^
.. autoclass:: TokenFilter
  :members:

Analyzer
^^^^^^^^^^^^^
.. autoclass:: Analyzer
  :members:


indexers
---------
.. automodule:: lupyne.engine.indexers

IndexReader
^^^^^^^^^^^^^
.. autoclass:: IndexReader
  :members:

  .. automethod:: __len__

  .. automethod:: __contains__

  .. automethod:: __iter__

IndexSearcher
^^^^^^^^^^^^^
.. autoclass:: IndexSearcher
  :members:

  .. automethod:: __getitem__

    Return `Document`_.

  .. automethod:: __del__

    Closes index.

  .. attribute:: spellcheckers

    Mapping of cached spellcheckers by field.

MultiSearcher
^^^^^^^^^^^^^
.. autoclass:: MultiSearcher
  :show-inheritance:

IndexWriter
^^^^^^^^^^^^^
.. autoclass:: IndexWriter
  :members:

  .. attribute:: fields

    Mapping of assigned fields.  May be used directly, instead of :meth:`set` method, for further customization.

  .. automethod:: __del__

    Closes index.

  .. automethod:: __len__

  .. automethod:: __iadd__

Indexer
^^^^^^^^^^^^^
.. autoclass:: Indexer
  :show-inheritance:
  :members:


documents
---------
.. automodule:: lupyne.engine.documents

Document
^^^^^^^^^^^^^
.. versionchanged:: 1.5 stored numeric types returned as numbers
.. autoclass:: Document
  :show-inheritance:
  :members:

Hit
^^^^^^^^^^^^^
.. autoclass:: Hit
  :show-inheritance:
  :members:

Hits
^^^^^^^^^^^^^
.. autoclass:: Hits
  :members:

  .. automethod:: __len__

  .. automethod:: __getitem__

Groups
^^^^^^^^^^^^^
.. versionadded:: 1.6
.. note:: This interface is experimental and might change in incompatible ways in the next release.
.. autoclass:: Groups
  :members:

  .. automethod:: __len__

  .. automethod:: __getitem__

GroupingSearch
^^^^^^^^^^^^^^
.. versionadded:: 1.5
.. note:: This interface is experimental and might change in incompatible ways in the next release.
.. autoclass:: GroupingSearch
  :members:

  .. automethod:: __len__

  .. automethod:: __iter__

Field
^^^^^^^^^^^^^
.. versionchanged:: 1.6 lucene Field.{Store,Index,TermVector} dropped in favor of FieldType attributes
.. autoclass:: Field
  :members:

NestedField
^^^^^^^^^^^^^
.. autoclass:: NestedField
  :show-inheritance:
  :members:

DateTimeField
^^^^^^^^^^^^^
.. autoclass:: DateTimeField
  :show-inheritance:
  :members:

SpatialField
^^^^^^^^^^^^^
.. autoclass:: SpatialField
  :show-inheritance:
  :members:


queries
---------
.. automodule:: lupyne.engine.queries

Query
^^^^^^^^^^^^^
.. autoclass:: Query
  :members:
  :special-members:
  :exclude-members: __weakref__

SpanQuery
^^^^^^^^^^^^^
.. autoclass:: SpanQuery
  :show-inheritance:
  :members:

  .. automethod:: __getitem__

    <SpanFirstQuery: spanFirst(self, other.stop)>

  .. automethod:: __sub__

    <SpanNotQuery: spanNot(self, other)>

  .. automethod:: __or__

    <SpanOrQuery: spanOr(spans)>

SpellParser
^^^^^^^^^^^^^
.. autoclass:: SpellParser
  :members:

  .. attribute:: searcher

    `IndexSearcher`_
