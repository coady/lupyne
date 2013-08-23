engine
======
.. automodule:: lupyne.engine

  * `TokenFilter`_, `Analyzer`_, `IndexSearcher`_, `MultiSearcher`_, `IndexWriter`_, `Indexer`_, `ParallelIndexer`_
  * `Document`_, `Field`_, `MapField`_, `NestedField`_, `NumericField`_, `DateTimeField`_
  * `Query`_, `SortField`_, `TermsFilter`_
  * `PointField`_, `PolygonField`_


indexers
---------
.. automodule:: lupyne.engine.indexers

TokenStream
^^^^^^^^^^^^^
.. autoclass:: TokenStream
  :members:

TokenFilter
^^^^^^^^^^^^^
.. autoclass:: TokenFilter
  :show-inheritance:
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

    Mapping of cached filters by field, also used for facet counts.

  .. attribute:: groupings

    Mapping of cached groupings by field, optimized for facet counts of unique fields.

  .. attribute:: sorters

    Mapping of cached sorters by field and associated parsers.

  .. attribute:: spellcheckers

    Mapping of cached spellcheckers by field.

  .. attribute:: termsfilters

    Set of registered termsfilters.

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

ParallelIndexer
^^^^^^^^^^^^^^^
.. versionadded:: 1.2
.. autoclass:: ParallelIndexer
  :show-inheritance:
  :members:

  .. attribute:: termsfilters

    Mapping of filters to synchronized termsfilters.


documents
---------
.. automodule:: lupyne.engine.documents

Document
^^^^^^^^^^^^^
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

Grouping
^^^^^^^^^^^^^
.. versionadded:: 1.3
.. autoclass:: Grouping
  :members:

  .. automethod:: __len__

  .. automethod:: __iter__

Field
^^^^^^^^^^^^^
.. autoclass:: Field
  :members:

MapField
^^^^^^^^^^^^^
.. autoclass:: MapField
  :show-inheritance:
  :members:

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

TermsFilter
^^^^^^^^^^^^^
.. versionadded:: 1.2
.. autoclass:: TermsFilter
  :show-inheritance:
  :members:

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
.. autoclass:: PointField
  :show-inheritance:
  :members:

PolygonField
^^^^^^^^^^^^^
.. autoclass:: PolygonField
  :show-inheritance:
  :members:

DistanceComparator
^^^^^^^^^^^^^^^^^^
.. autoclass:: DistanceComparator
  :show-inheritance:
  :members:

  .. automethod:: __getitem__
