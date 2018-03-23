import collections
from java.io import StringReader
from java.lang import Float
from java.util import HashMap
from org.apache.lucene import analysis, queryparser, util
from org.apache.lucene.search import uhighlight
from org.apache.pylucene.analysis import PythonAnalyzer, PythonTokenFilter
from org.apache.pylucene.queryparser.classic import PythonQueryParser
from six import string_types
from .utils import method


class TokenStream(analysis.TokenStream):
    """TokenStream mixin with support for iteration and attributes cached as properties."""
    def __iter__(self):
        self.reset()
        return self

    def __next__(self):
        if self.incrementToken():
            return self
        raise StopIteration
    next = __next__

    def __getattr__(self, name):
        cls = getattr(analysis.tokenattributes, name + 'Attribute').class_
        attr = self.getAttribute(cls) if self.hasAttribute(cls) else self.addAttribute(cls)
        setattr(self, name, attr)
        return attr

    @property
    def offset(self):
        """start and stop character offset"""
        return self.Offset.startOffset(), self.Offset.endOffset()

    @offset.setter
    def offset(self, item):
        self.Offset.setOffset(*item)

    @property
    def payload(self):
        """payload bytes"""
        payload = self.Payload.payload
        return payload and payload.utf8ToString()

    @payload.setter
    def payload(self, data):
        self.Payload.payload = util.BytesRef(data)

    @property
    def positionIncrement(self):
        """position relative to the previous token"""
        return self.PositionIncrement.positionIncrement

    @positionIncrement.setter
    def positionIncrement(self, index):
        self.PositionIncrement.positionIncrement = index

    @property
    def charTerm(self):
        """term text"""
        return self.CharTerm.toString()

    @charTerm.setter
    def charTerm(self, text):
        self.CharTerm.setEmpty()
        self.CharTerm.append(text)

    @property
    def type(self):
        """lexical type"""
        return self.Type.type()

    @type.setter
    def type(self, text):
        self.Type.setType(text)


class TokenFilter(PythonTokenFilter, TokenStream):
    """Create an iterable lucene TokenFilter from a TokenStream.

    Subclass and override :meth:`incrementToken`.
    """
    def __init__(self, input):
        PythonTokenFilter.__init__(self, input)
        self.input = input

    def incrementToken(self):
        """Advance to next token and return whether the stream is not empty."""
        return self.input.incrementToken()


class Analyzer(PythonAnalyzer):
    """Return a lucene Analyzer which chains together a tokenizer and filters.

    :param tokenizer: lucene Tokenizer class or callable, called with no args
    :param filters: lucene TokenFilter classes or callables, successively called on input tokens
    """
    def __init__(self, tokenizer, *filters):
        PythonAnalyzer.__init__(self)
        self.tokenizer, self.filters = tokenizer, filters

    @classmethod
    def standard(cls, *filters):
        """Return equivalent of StandardAnalyzer with additional filters."""
        def stop(tokens):
            return analysis.StopFilter(tokens, analysis.standard.StandardAnalyzer.STOP_WORDS_SET)
        return cls(analysis.standard.StandardTokenizer, analysis.standard.StandardFilter, analysis.LowerCaseFilter, stop, *filters)

    @classmethod
    def whitespace(cls, *filters):
        """Return equivalent of WhitespaceAnalyzer with additional filters."""
        return cls(analysis.core.WhitespaceTokenizer, *filters)

    def components(self, field, reader=None):
        source = tokens = self.tokenizer()
        if reader is not None:
            source.reader = reader
        for filter in self.filters:
            tokens = filter(tokens)
        return source, tokens

    def createComponents(self, field):
        return analysis.Analyzer.TokenStreamComponents(*self.components(field))

    def tokens(self, text, field=None):
        """Return lucene TokenStream from text."""
        return self.components(field, StringReader(text))[1]

    @method
    def parse(self, query, field='', op='', parser=None, **attrs):
        """Return parsed lucene Query.

        :param query: query string
        :param field: default query field name, sequence of names, or boost mapping
        :param op: default query operator ('or', 'and')
        :param parser: custom PythonQueryParser class
        :param attrs: additional attributes to set on the parser
        """
        # parsers aren't thread-safe (nor slow), so create one each time
        cls = queryparser.classic.QueryParser if isinstance(field, string_types) else queryparser.classic.MultiFieldQueryParser
        args = field, self
        if isinstance(field, collections.Mapping):
            boosts = HashMap()
            for key in field:
                boosts.put(key, Float(field[key]))
            args = list(field), self, boosts
        parser = (parser or cls)(*args)
        if op:
            parser.defaultOperator = getattr(queryparser.classic.QueryParser.Operator, op.upper())
        for name, value in attrs.items():
            setattr(parser, name, value)
        if isinstance(parser, queryparser.classic.MultiFieldQueryParser):
            return parser.parse(parser, query)
        try:
            return parser.parse(query)
        finally:
            if isinstance(parser, PythonQueryParser):
                parser.finalize()

    @method
    def highlight(self, query, field, content, count=1):
        """Return highlighted content.

        :param query: lucene Query
        :param field: field name
        :param content: text
        :param count: optional maximum number of passages
        """
        return uhighlight.UnifiedHighlighter(None, self).highlightWithoutSearcher(field, query, content, count).toString()
