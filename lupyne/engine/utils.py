import abc
import bisect
import contextlib
import heapq
import itertools
from typing import Iterable, Optional
import lucene
from java.lang import Double, Float, Number, Object
from org.apache.lucene import analysis, util


class Atomic(metaclass=abc.ABCMeta):
    """Abstract base class to distinguish singleton values from other iterables."""

    @classmethod
    def __subclasshook__(cls, other):
        return not issubclass(other, Iterable) or NotImplemented


for cls in (str, analysis.TokenStream, lucene.JArray_byte):
    Atomic.register(cls)


class SpellChecker(dict):
    """Correct spellings and suggest words for queries.

    Supply a vocabulary mapping words to (reverse) sort keys, such as document frequencies.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.words = sorted(self)
        alphabet = ''.join(set(itertools.chain.from_iterable(self)))
        self.suffix = alphabet and max(alphabet) * max(map(len, self))

    def complete(self, prefix: str, count: Optional[int] = None) -> list:
        """Return ordered suggested words for prefix."""
        start = bisect.bisect_left(self.words, prefix)
        stop = bisect.bisect_right(self.words, prefix + self.suffix, start)
        words = self.words[start:stop]
        if count is not None and count < len(words):
            return heapq.nlargest(count, words, key=self.__getitem__)
        words.sort(key=self.__getitem__, reverse=True)
        return words


@contextlib.contextmanager
def suppress(exception):
    """Suppress specific lucene exception."""
    try:
        yield
    except lucene.JavaError as exc:
        if not exception.instance_(exc.getJavaException()):
            raise


def convert(value):
    """Return python object from java Object."""
    if util.BytesRef.instance_(value):
        return util.BytesRef.cast_(value).utf8ToString()
    if not Number.instance_(value):
        return value.toString() if Object.instance_(value) else value
    value = Number.cast_(value)
    return value.doubleValue() if Float.instance_(value) or Double.instance_(value) else int(value.longValue())
