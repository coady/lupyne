"""
Query wrappers and search utilities.
"""

from future_builtins import filter, map
import itertools
import bisect
import heapq
import threading
import lucene

class Query(object):
    """Inherited lucene Query, with dynamic base class acquisition.
    Uses class methods and operator overloading for convenient query construction.
    """
    def __new__(cls, base, *args):
        return base.__new__(type(base.__name__, (cls, base), {}))
    def __init__(self, base, *args):
        base.__init__(self, *args)
    def filter(self, cache=True):
        "Return lucene CachingWrapperFilter, optionally just QueryWrapperFilter."
        if isinstance(self, lucene.PrefixQuery):
            filter = lucene.PrefixFilter(self.getPrefix())
        elif isinstance(self, lucene.TermRangeQuery):
            filter = lucene.TermRangeFilter(self.field, self.lowerTerm, self.upperTerm, self.includesLower(), self.includesUpper())
        elif isinstance(self, lucene.TermQuery):
            filter = lucene.TermsFilter()
            filter.addTerm(self.getTerm())
        else:
            filter = lucene.QueryWrapperFilter(self)
        return lucene.CachingWrapperFilter(filter) if cache else filter
    def terms(self):
        "Generate set of query term items."
        terms = lucene.HashSet().of_(lucene.Term)
        self.extractTerms(terms)
        for term in terms:
            yield term.field(), term.text()
    @classmethod
    def term(cls, name, value, boost=1.0):
        "Return lucene TermQuery."
        self = cls(lucene.TermQuery, lucene.Term(name, value))
        self.boost = boost
        return self
    @classmethod
    def boolean(cls, occur, *queries, **terms):
        self = BooleanQuery(lucene.BooleanQuery)
        for query in queries:
            self.add(query, occur)
        for name, values in terms.items():
            for value in ([values] if isinstance(values, basestring) else values):
                self.add(cls.term(name, value), occur)
        return self
    @classmethod
    def any(cls, *queries, **terms):
        "Return `BooleanQuery`_ (OR) from queries and terms."
        return cls.boolean(lucene.BooleanClause.Occur.SHOULD, *queries, **terms)
    @classmethod
    def all(cls, *queries, **terms):
        "Return `BooleanQuery`_ (AND) from queries and terms."
        return cls.boolean(lucene.BooleanClause.Occur.MUST, *queries, **terms)
    @classmethod
    def disjunct(cls, multiplier, *queries, **terms):
        "Return lucene DisjunctionMaxQuery from queries and terms."
        self = cls(lucene.DisjunctionMaxQuery, lucene.Arrays.asList(queries), multiplier)
        for name, values in terms.items():
            for value in ([values] if isinstance(values, basestring) else values):
                self.add(cls.term(name, value))
        return self
    @classmethod
    def span(cls, *term):
        "Return `SpanQuery`_ from term name and value or a MultiTermQuery."
        if len(term) <= 1:
            return SpanQuery(lucene.SpanMultiTermQueryWrapper, *term)
        return SpanQuery(lucene.SpanTermQuery, lucene.Term(*term))
    @classmethod
    def near(cls, name, *values, **kwargs):
        """Return :meth:`SpanNearQuery <SpanQuery.near>` from terms.
        Term values which supply another field name will be masked."""
        spans = (cls.span(name, value) if isinstance(value, basestring) else cls.span(*value).mask(name) for value in values)
        return SpanQuery.near(*spans, **kwargs)
    @classmethod
    def prefix(cls, name, value):
        "Return lucene PrefixQuery."
        return cls(lucene.PrefixQuery, lucene.Term(name, value))
    @classmethod
    def range(cls, name, start, stop, lower=True, upper=False):
        "Return lucene RangeQuery, by default with a half-open interval."
        return cls(lucene.TermRangeQuery, name, start, stop, lower, upper)
    @classmethod
    def phrase(cls, name, *values):
        "Return lucene PhraseQuery.  None may be used as a placeholder."
        self = cls(lucene.PhraseQuery)
        for index, value in enumerate(values):
            if value is not None:
                self.add(lucene.Term(name, value), index)
        return self
    @classmethod
    def multiphrase(cls, name, *values):
        "Return lucene MultiPhraseQuery.  None may be used as a placeholder."
        self = cls(lucene.MultiPhraseQuery)
        for index, words in enumerate(values):
            if isinstance(words, basestring):
                words = [words]
            if words is not None:
                self.add([lucene.Term(name, word) for word in words], index)
        return self
    @classmethod
    def wildcard(cls, name, value):
        "Return lucene WildcardQuery."
        return cls(lucene.WildcardQuery, lucene.Term(name, value))
    @classmethod
    def fuzzy(cls, name, value, minimumSimilarity=0.5, prefixLength=0):
        "Return lucene FuzzyQuery."
        return cls(lucene.FuzzyQuery, lucene.Term(name, value), minimumSimilarity, prefixLength)
    def __pos__(self):
        return Query.all(self)
    def __neg__(self):
        return Query.boolean(lucene.BooleanClause.Occur.MUST_NOT, self)
    def __and__(self, other):
        return Query.all(self, other)
    def __rand__(self, other):
        return Query.all(other, self)
    def __or__(self, other):
        return Query.any(self, other)
    def __ror__(self, other):
        return Query.any(other, self)
    def __sub__(self, other):
        return Query.any(self).__isub__(other)
    def __rsub__(self, other):
        return Query.any(other).__isub__(self)

class BooleanQuery(Query):
    "Inherited lucene BooleanQuery with sequence interface to clauses."
    def __len__(self):
        return len(self.getClauses())
    def __iter__(self):
        return iter(self.getClauses())
    def __getitem__(self, index):
        return self.getClauses()[index]
    def __iand__(self, other):
        self.add(other, lucene.BooleanClause.Occur.MUST)
        return self
    def __ior__(self, other):
        self.add(other, lucene.BooleanClause.Occur.SHOULD)
        return self
    def __isub__(self, other):
        self.add(other, lucene.BooleanClause.Occur.MUST_NOT)
        return self

class SpanQuery(Query):
    "Inherited lucene SpanQuery with additional span constructors."
    def filter(self, cache=True):
        "Return lucene CachingSpanFilter, optionally just SpanQueryFilter."
        filter = lucene.SpanQueryFilter(self)
        return lucene.CachingSpanFilter(filter) if cache else filter
    def __getitem__(self, slc):
        start, stop, step = slc.indices(lucene.Integer.MAX_VALUE)
        assert step == 1, 'slice step is not supported'
        return SpanQuery(lucene.SpanPositionRangeQuery, self, start, stop)
    def __sub__(self, other):
        return SpanQuery(lucene.SpanNotQuery, self, other)
    def __or__(*spans):
        return SpanQuery(lucene.SpanOrQuery, spans)
    def near(*spans, **kwargs):
        """Return lucene SpanNearQuery from SpanQueries.
        
        :param slop: default 0
        :param inOrder: default True
        :param collectPayloads: default True
        """
        args = map(kwargs.get, ('slop', 'inOrder', 'collectPayloads'), (0, True, True))
        return SpanQuery(lucene.SpanNearQuery, spans, *args)
    def mask(self, name):
        "Return lucene FieldMaskingSpanQuery, which allows combining span queries from different fields."
        return SpanQuery(lucene.FieldMaskingSpanQuery, self, name)
    def payload(self, *values):
        "Return lucene SpanPayloadCheckQuery from payload values."
        base = lucene.SpanNearPayloadCheckQuery if lucene.SpanNearQuery.instance_(self) else lucene.SpanPayloadCheckQuery
        return SpanQuery(base, self, lucene.Arrays.asList(list(map(lucene.JArray_byte, values))))

class Collector(lucene.PythonCollector):
    "Collect all ids and scores efficiently."
    def __init__(self):
        lucene.PythonCollector.__init__(self)
        self.scores = {}
    def collect(self, id, score):
        self.scores[id + self.base] = score
    def setNextReader(self, reader, base):
        self.base = base
    def acceptsDocsOutOfOrder(self):
        return True
    def sorted(self, key=None, reverse=False):
        "Return ordered ids and scores."
        ids = sorted(self.scores)
        if key is None:
            key, reverse = self.scores.__getitem__, True
        ids.sort(key=key, reverse=reverse)
        return ids, list(map(self.scores.__getitem__, ids))

class TermsFilter(lucene.CachingWrapperFilter):
    """Caching filter based on a unique field and set of matching values.
    Optimized for many terms and docs, with support for incremental updates.
    Suitable for searching external metadata associated with indexed identifiers.
    Call :meth:`refresh` to cache a new (or reopened) reader.
    
    :param field: field name
    :param values: initial term values, synchronized with the cached filters
    """
    ops = {'or': 'update', 'and': 'intersection_update', 'andNot': 'difference_update'}
    def __init__(self, field, values=()):
        assert hasattr(lucene, 'FixedBitSet'), 'requires FixedBitSet introduced in lucene 3.4'
        lucene.CachingWrapperFilter.__init__(self, lucene.TermsFilter())
        self.field = field
        self.values = set(values)
        self.readers = set()
        self.lock = threading.Lock()
    def filter(self, values, cache=True):
        "Return lucene TermsFilter, optionally using the FieldCache."
        if cache:
            return lucene.FieldCacheTermsFilter(self.field, tuple(values))
        filter, term = lucene.TermsFilter(), lucene.Term(self.field)
        for value in values:
            filter.addTerm(term.createTerm(value))
        return filter
    def apply(self, filter, op, readers):
        for reader in readers:
            bitset = lucene.FixedBitSet.cast_(self.getDocIdSet(reader))
            getattr(bitset, op)(filter.getDocIdSet(reader).iterator())
    def update(self, values, op='or', cache=True):
        """Update allowed values and corresponding cached bitsets.
        
        :param values: additional term values
        :param op: set operation used to combine terms and docs
        :param cache: optionally cache all term values using FieldCache
        """
        values = tuple(values)
        filter = self.filter(values, cache)
        with self.lock:
            getattr(self.values, self.ops[op])(values)
            self.apply(filter, op, self.readers)
    def refresh(self, reader):
        "Refresh cached bitsets of current values for new segments of top-level reader."
        readers = set(reader.sequentialSubReaders)
        with self.lock:
            self.apply(self.filter(self.values), 'or', readers - self.readers)
            self.readers = set(reader for reader in readers | self.readers if reader.refCount)
    def add(self, *values):
        "Add a few term values."
        self.update(values, cache=False)
    def discard(self, *values):
        "Discard a few term values."
        self.update(values, op='andNot', cache=False)

class SortField(lucene.SortField):
    """Inherited lucene SortField used for caching FieldCache parsers.
    
    :param name: field name
    :param type: type object or name compatible with SortField constants
    :param parser: lucene FieldCache.Parser or callable applied to field values
    :param reverse: reverse flag used with sort
    """
    def __init__(self, name, type='string', parser=None, reverse=False):
        type = self.typename = getattr(type, '__name__', type).capitalize()
        if parser is None:
            parser = getattr(lucene.SortField, type.upper())
        elif not lucene.FieldCache.Parser.instance_(parser):
            base = getattr(lucene, 'Python{0}Parser'.format(type))
            namespace = {'parse' + type: staticmethod(parser)}
            parser = object.__class__(base.__name__, (base,), namespace)()
        lucene.SortField.__init__(self, name, parser, reverse)
    def array(self, reader):
        method = getattr(lucene.FieldCache.DEFAULT, 'get{0}s'.format(self.typename))
        return method(reader, self.field, *[self.parser][:bool(self.parser)])
    def comparator(self, reader):
        "Return indexed values from default FieldCache using the given top-level reader."
        readers = reader.sequentialSubReaders
        if lucene.MultiReader.instance_(reader):
            readers = itertools.chain.from_iterable(reader.sequentialSubReaders for reader in readers)
        arrays = list(map(self.array, readers))
        if len(arrays) <= 1:
            return arrays[0]
        cls, = set(map(type, arrays))
        index, result = 0, cls(sum(map(len, arrays)))
        for array in arrays:
            lucene.System.arraycopy(array, 0, result, index, len(array))
            index += len(array)
        return result
    def filter(self, start, stop, lower=True, upper=False):
        "Return lucene FieldCacheRangeFilter based on field and type."
        method = getattr(lucene.FieldCacheRangeFilter, 'new{0}Range'.format(self.typename))
        return method(self.field, self.parser, start, stop, lower, upper)
    def terms(self, filter, *readers):
        "Generate field cache terms from docs which match filter from all segments."
        for reader in readers:
            array, it = self.array(reader), filter.getDocIdSet(reader).iterator()
            try:
                while True:
                    yield array[it.nextDoc()]
            except IndexError:
                pass

class Highlighter(lucene.Highlighter):
    """Inherited lucene Highlighter with stored analysis options.
    
    :param searcher: `IndexSearcher`_ used for analysis, scoring, and optionally text retrieval
    :param query: lucene Query
    :param field: field name of text
    :param terms: highlight any matching term in query regardless of position
    :param fields: highlight matching terms from any field
    :param tag: optional html tag name
    :param formatter: optional lucene Formatter
    :param encoder: optional lucene Encoder
    """
    def __init__(self, searcher, query, field, terms=False, fields=False, tag='', formatter=None, encoder=None):
        if tag:
            formatter = lucene.SimpleHTMLFormatter('<{0}>'.format(tag), '</{0}>'.format(tag))
        scorer = (lucene.QueryTermScorer if terms else lucene.QueryScorer)(query, *(searcher.indexReader, field) * (not fields))
        lucene.Highlighter.__init__(self, *filter(None, [formatter, encoder, scorer]))
        self.searcher, self.field = searcher, field
        self.selector = lucene.MapFieldSelector([field])
    def fragments(self, doc, count=1):
        """Return highlighted text fragments.
        
        :param doc: text string or doc id to be highlighted
        :param count: maximum number of fragments
        """
        if not isinstance(doc, basestring):
            doc = self.searcher.doc(doc, self.selector)[self.field]
        return doc and list(self.getBestFragments(self.searcher.analyzer, self.field, doc, count))

class FastVectorHighlighter(lucene.FastVectorHighlighter):
    """Inherited lucene FastVectorHighlighter with stored query.
    Fields must be stored and have term vectors with offsets and positions.
    
    :param searcher: `IndexSearcher`_ with stored term vectors
    :param query: lucene Query
    :param field: field name of text
    :param terms: highlight any matching term in query regardless of position
    :param fields: highlight matching terms from any field
    :param tag: optional html tag name
    :param fragListBuilder: optional lucene FragListBuilder
    :param fragmentsBuilder: optional lucene FragmentsBuilder
    """
    def __init__(self, searcher, query, field, terms=False, fields=False, tag='', fragListBuilder=None, fragmentsBuilder=None):
        if tag:
            fragmentsBuilder = lucene.SimpleFragmentsBuilder(['<{0}>'.format(tag)], ['</{0}>'.format(tag)])
        args = fragListBuilder or lucene.SimpleFragListBuilder(), fragmentsBuilder or lucene.SimpleFragmentsBuilder()
        lucene.FastVectorHighlighter.__init__(self, not terms, not fields, *args)
        self.searcher, self.field = searcher, field
        self.query = self.getFieldQuery(query)
    def fragments(self, id, count=1, size=100):
        """Return highlighted text fragments.
        
        :param id: document id
        :param count: maximum number of fragments
        :param size: maximum number of characters in fragment
        """
        return list(self.getBestFragments(self.query, self.searcher.indexReader, id, self.field, size, count))

class SpellChecker(dict):
    """Correct spellings and suggest words for queries.
    Supply a vocabulary mapping words to (reverse) sort keys, such as document frequencies.
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.words = sorted(self)
        self.alphabet = sorted(set(itertools.chain.from_iterable(self.words)))
        self.suffix = self.alphabet[-1] * max(map(len, self.words)) if self.alphabet else ''
        self.prefixes = set(word[:stop] for word in self.words for stop in range(len(word) + 1))
    def suggest(self, prefix, count=None):
        "Return ordered suggested words for prefix."
        start = bisect.bisect_left(self.words, prefix)
        stop = bisect.bisect_right(self.words, prefix + self.suffix, start)
        words = self.words[start:stop]
        if count is not None and count < len(words):
            return heapq.nlargest(count, words, key=self.__getitem__)
        words.sort(key=self.__getitem__, reverse=True)
        return words
    def edits(self, word, length=0):
        "Return set of potential words one edit distance away, mapped to valid prefix lengths."
        pairs = [(word[:index], word[index:]) for index in range(len(word) + 1)]
        deletes = (head + tail[1:] for head, tail in pairs[:-1])
        transposes = (head + tail[1::-1] + tail[2:] for head, tail in pairs[:-2])
        edits = {} if length else dict.fromkeys(itertools.chain(deletes, transposes), 0)
        for head, tail in pairs[length:]:
            if head not in self.prefixes:
                break
            for char in self.alphabet:
                prefix = head + char
                if prefix in self.prefixes:
                    edits[prefix + tail] = edits[prefix + tail[1:]] = len(prefix)
        return edits
    def correct(self, word):
        "Generate ordered sets of words by increasing edit distance."
        previous, edits = set(), {word: 0}
        for distance in range(len(word)):
            yield sorted(filter(self.__contains__, edits), key=self.__getitem__, reverse=True)
            previous.update(edits)
            groups = map(self.edits, edits, edits.values())
            edits = dict((edit, group[edit]) for group in groups for edit in group if edit not in previous)

class SpellParser(lucene.PythonQueryParser):
    """Inherited lucene QueryParser which corrects spelling.
    Assign a searcher attribute or override :meth:`correct` implementation.
    """
    def correct(self, term):
        "Return term with text replaced as necessary."
        field = term.field()
        for text in self.searcher.correct(field, term.text()):
            return lucene.Term(field, text)
        return term
    def rewrite(self, query):
        "Return term or phrase query with corrected terms substituted."
        if lucene.TermQuery.instance_(query):
            term = lucene.TermQuery.cast_(query).term
            return lucene.TermQuery(self.correct(term))
        query = lucene.PhraseQuery.cast_(query)
        phrase = lucene.PhraseQuery()
        for position, term in zip(query.positions, query.terms):
            phrase.add(self.correct(term), position)
        return phrase
    def getFieldQuery_quoted(self, *args):
        return self.rewrite(self.getFieldQuery_quoted_super(*args))
    def getFieldQuery_slop(self, *args):
        return self.rewrite(self.getFieldQuery_slop_super(*args))
