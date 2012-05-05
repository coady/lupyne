engine
======
.. automodule:: lupyne.engine


indexers
---------
.. automodule:: lupyne.engine.indexers

TokenStream
^^^^^^^^^^^^^
.. autoclass:: TokenStream
  :members:

TokenFilter
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.TokenFilter
  :show-inheritance:
  :members:

Analyzer
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.Analyzer
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
.. autoclass:: lupyne.engine.IndexSearcher
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

  .. attribute:: termsfilters

    Set of registered termsfilters.

MultiSearcher
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.MultiSearcher
  :show-inheritance:

IndexWriter
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.IndexWriter
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
.. autoclass:: lupyne.engine.Indexer
  :show-inheritance:
  :members:


documents
---------
.. automodule:: lupyne.engine.documents

Document
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.Document
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

Field
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.Field
  :members:

FormatField
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.FormatField
  :show-inheritance:
  :members:

  .. method:: format(value)

    Return formatted value.

NestedField
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.NestedField
  :show-inheritance:
  :members:

NumericField
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.NumericField
  :show-inheritance:
  :members:

DateTimeField
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.DateTimeField
  :show-inheritance:
  :members:


queries
---------
.. automodule:: lupyne.engine.queries

Query
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.Query
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

TermsFilter
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.TermsFilter
  :show-inheritance:
  :members:

SortField
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.SortField
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
  :show-inheritance:
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
.. autoclass:: lupyne.engine.PointField
  :show-inheritance:
  :members:

PolygonField
^^^^^^^^^^^^^
.. autoclass:: lupyne.engine.PolygonField
  :show-inheritance:
  :members:

DistanceComparator
^^^^^^^^^^^^^^^^^^
.. autoclass:: DistanceComparator
  :show-inheritance:
  :members:

  .. automethod:: __getitem__
