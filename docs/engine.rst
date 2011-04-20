engine
======
.. automodule:: engine


indexers
---------
.. automodule:: engine.indexers

TokenFilter
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.TokenFilter
  :members:

Analyzer
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.Analyzer
  :members:

IndexReader
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexReader
  :members:

  .. automethod:: __len__

  .. automethod:: __contains__

  .. automethod:: __iter__

  .. automethod:: __getitem__

IndexSearcher
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.IndexSearcher
  :show-inheritance:
  :members:

  .. automethod:: __getitem__

    Return `Document`_.

  .. automethod:: __del__

    Closes index.

  .. attribute:: filters

    Mapping of cached filters by field, which are used for facet counts.

  .. attribute:: sorters

    Mapping of cached sorters by field and associated parsers.

  .. attribute:: spellcheckers

    Mapping of cached spellcheckers by field.

MultiSearcher
^^^^^^^^^^^^^
.. autoclass:: engine.indexers.MultiSearcher
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

Hit
^^^^^^^^^^^^^
.. autoclass:: engine.documents.Hit
  :show-inheritance:
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

NestedField
^^^^^^^^^^^^^
.. autoclass:: engine.documents.NestedField
  :show-inheritance:
  :members:

NumericField
^^^^^^^^^^^^^
.. autoclass:: engine.documents.NumericField
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

    `BooleanQuery`_ +self +other>

  .. automethod:: __or__

    `BooleanQuery`_ self other>

  .. automethod:: __sub__

    `BooleanQuery`_ self -other>

BooleanQuery
^^^^^^^^^^^^^
.. autoclass:: engine.queries.BooleanQuery
  :show-inheritance:
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
  :show-inheritance:
  :members:

  .. automethod:: __getitem__

    <SpanFirstQuery: spanFirst(self, other.stop)>

  .. automethod:: __sub__

    <SpanNotQuery: spanNot(self, other)>

  .. automethod:: __or__

    <SpanOrQuery: spanOr(spans)>

SortField
^^^^^^^^^^^^^
.. autoclass:: engine.queries.SortField
  :show-inheritance:
  :members:

Highlighter
^^^^^^^^^^^^^
.. autoclass:: engine.queries.Highlighter
  :show-inheritance:
  :members:

SpellChecker
^^^^^^^^^^^^^
.. autoclass:: engine.queries.SpellChecker
  :show-inheritance:
  :members:

SpellParser
^^^^^^^^^^^^^
.. autoclass:: engine.queries.SpellParser
  :members:

  .. attribute:: searcher

    `IndexSearcher`_


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
