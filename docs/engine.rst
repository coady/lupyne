engine
======
.. automodule:: lupyne.engine


indexers
---------
.. automodule:: lupyne.engine.indexers

TokenFilter
^^^^^^^^^^^^^
.. autoclass:: TokenFilter
  :members:

Analyzer
^^^^^^^^^^^^^
.. autoclass:: Analyzer
  :members:

IndexReader
^^^^^^^^^^^^^
.. autoclass:: IndexReader
  :members:

  .. automethod:: __len__

  .. automethod:: __contains__

  .. automethod:: __iter__

  .. automethod:: __getitem__

IndexSearcher
^^^^^^^^^^^^^
.. autoclass:: IndexSearcher
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
.. autoclass:: MultiSearcher
  :show-inheritance:

IndexWriter
^^^^^^^^^^^^^
.. autoclass:: IndexWriter
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
.. autoclass:: Indexer
  :show-inheritance:
  :members:


documents
---------
.. automodule:: lupyne.engine.documents

Document
^^^^^^^^^^^^^
.. autoclass:: Document
  :members:

  .. automethod:: __len__

  .. automethod:: __contains__

  .. automethod:: __iter__

  .. automethod:: __getitem__

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

Field
^^^^^^^^^^^^^
.. autoclass:: Field
  :members:

FormatField
^^^^^^^^^^^^^
.. autoclass:: FormatField
  :show-inheritance:
  :members:

  .. method:: format(value)

    Return formatted value.

NestedField
^^^^^^^^^^^^^
.. autoclass:: NestedField
  :show-inheritance:
  :members:

NumericField
^^^^^^^^^^^^^
.. autoclass:: NumericField
  :show-inheritance:
  :members:

DateTimeField
^^^^^^^^^^^^^
.. autoclass:: DateTimeField
  :show-inheritance:
  :members:


queries
---------
.. automodule:: lupyne.engine.queries

Query
^^^^^^^^^^^^^
.. autoclass:: Query
  :members:

  .. automethod:: __and__

    `BooleanQuery`_ +self +other>

  .. automethod:: __or__

    `BooleanQuery`_ self other>

  .. automethod:: __sub__

    `BooleanQuery`_ self -other>

BooleanQuery
^^^^^^^^^^^^^
.. autoclass:: BooleanQuery
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
.. autoclass:: SpanQuery
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
.. autoclass:: SortField
  :show-inheritance:
  :members:

Highlighter
^^^^^^^^^^^^^
.. autoclass:: Highlighter
  :show-inheritance:
  :members:

FastVectorHighlighter
^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: FastVectorHighlighter
  :members:

SpellChecker
^^^^^^^^^^^^^
.. autoclass:: SpellChecker
  :show-inheritance:
  :members:

SpellParser
^^^^^^^^^^^^^
.. autoclass:: SpellParser
  :members:

  .. attribute:: searcher

    `IndexSearcher`_


spatial
---------
.. automodule:: lupyne.engine.spatial

Tiler
^^^^^^^^^^^^^
.. autoclass:: Tiler
  :members:

PointField
^^^^^^^^^^^^^
.. autoclass:: PointField
  :show-inheritance:
  :members:

PolygonField
^^^^^^^^^^^^^
.. autoclass:: PolygonField
  :show-inheritance:
  :members:
