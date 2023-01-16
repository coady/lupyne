from typing import Callable, Iterable, Mapping, Optional
import jcc  # noqa: F401 needed for building docs
from java.io import StringReader
from java.lang import Float
from java.util import HashMap
from org.apache.lucene import analysis, queryparser, search, util
from org.apache.lucene.search import uhighlight
from org.apache.pylucene.analysis import PythonAnalyzer, PythonTokenFilter
from org.apache.pylucene.queryparser.classic import PythonQueryParser


class TokenStream(analysis.TokenStream):
    """TokenStream mixin with support for iteration and attributes cached as properties."""

    def __iter__(self):
        self.reset()
        return self

    def __next__(self):
        if self.incrementToken():
            return self
        raise StopIteration

    def __getattr__(self, name):
        cls = getattr(analysis.tokenattributes, name + 'Attribute').class_
        attr = self.getAttribute(cls) if self.hasAttribute(cls) else self.addAttribute(cls)
        setattr(self, name, attr)
        return attr

    @property
    def offset(self) -> tuple:
        """start and stop character offset"""
        return self.Offset.startOffset(), self.Offset.endOffset()

    @offset.setter
    def offset(self, item: Iterable):
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
    def positionIncrement(self) -> int:
        """position relative to the previous token"""
        return self.PositionIncrement.positionIncrement

    @positionIncrement.setter
    def positionIncrement(self, index: int):
        self.PositionIncrement.positionIncrement = index

    @property
    def charTerm(self) -> str:
        """term text"""
        return self.CharTerm.toString()

    @charTerm.setter
    def charTerm(self, text: str):
        self.CharTerm.setEmpty()
        self.CharTerm.append(text)

    @property
    def type(self) -> str:
        """lexical type"""
        return self.Type.type()

    @type.setter
    def type(self, text: str):
        self.Type.setType(text)


class TokenFilter(PythonTokenFilter, TokenStream):
    """Create an iterable lucene TokenFilter from a TokenStream.

    Subclass and override [incrementToken][lupyne.engine.analyzers.TokenFilter.incrementToken].
    """

    def __init__(self, input: analysis.TokenStream):
        super().__init__(input)
        self.input = input

    def incrementToken(self) -> bool:
        """Advance to next token and return whether the stream is not empty."""
        return self.input.incrementToken()


class Analyzer(PythonAnalyzer):
    """Return a lucene Analyzer which chains together a tokenizer and filters.

    Args:
        tokenizer: lucene Tokenizer class or callable, called with no args
        *filters: lucene TokenFilter classes or callables, successively called on input tokens
    """

    def __init__(self, tokenizer: Callable, *filters: Callable):
        super().__init__()
        self.tokenizer, self.filters = tokenizer, filters

    @classmethod
    def standard(cls, *filters: Callable) -> 'Analyzer':
        """Return equivalent of StandardAnalyzer with additional filters."""
        return cls(analysis.standard.StandardTokenizer, analysis.LowerCaseFilter, *filters)

    @classmethod
    def whitespace(cls, *filters: Callable) -> 'Analyzer':
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

    def tokens(self, text: str, field: Optional[str] = None) -> analysis.TokenStream:
        """Return lucene TokenStream from text."""
        return self.components(field, StringReader(text))[1]

    def parse(self, query: str, field='', op='', parser=None, **attrs) -> search.Query:
        """Return parsed lucene Query.

        Args:
            query: query string
            field: default query field name, sequence of names, or boost mapping
            op: default query operator ('or', 'and')
            parser: custom PythonQueryParser class
            **attrs: additional attributes to set on the parser
        """
        # parsers aren't thread-safe (nor slow), so create one each time
        cls = queryparser.classic.QueryParser if isinstance(field, str) else queryparser.classic.MultiFieldQueryParser
        args = field, self
        if isinstance(field, Mapping):
            boosts = HashMap()
            for key in field:
                boosts.put(key, Float(field[key]))
            args = list(field), self, boosts  # type: ignore
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

    def highlight(self, query: search.Query, field: str, content: str, count: int = 1):
        """Return highlighted content.

        Args:
            query: lucene Query
            field: field name
            content: text
            count: optional maximum number of passages
        """
        highlighter = uhighlight.UnifiedHighlighter(None, self)
        return highlighter.highlightWithoutSearcher(field, query, content, count).toString()
