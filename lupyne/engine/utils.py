import abc
import bisect
import collections
import contextlib
import heapq
import itertools
import types
import lucene
import six
from java.lang import Double, Float, Number, Object
from org.apache.lucene import analysis, util
long = int if six.PY3 else long  # noqa


class Atomic(six.with_metaclass(abc.ABCMeta)):
    """Abstract base class to distinguish singleton values from other iterables."""
    @classmethod
    def __subclasshook__(cls, other):
        return not issubclass(other, collections.Iterable) or NotImplemented


for cls in six.string_types + (analysis.TokenStream, lucene.JArray_byte):
    Atomic.register(cls)


class method(staticmethod):
    """Backport of Python 3's unbound methods."""
    def __get__(self, instance, owner):
        return self.__func__ if instance is None else types.MethodType(self.__func__, instance)


class SpellChecker(dict):
    """Correct spellings and suggest words for queries.

    Supply a vocabulary mapping words to (reverse) sort keys, such as document frequencies.
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.words = sorted(self)
        alphabet = ''.join(set(itertools.chain.from_iterable(self)))
        self.suffix = alphabet and max(alphabet) * max(map(len, self))

    def complete(self, prefix, count=None):
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
