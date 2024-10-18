import abc
import contextlib
from collections.abc import Iterable
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


@contextlib.contextmanager
def suppress(exception):
    """Suppress specific lucene exception."""
    try:
        yield
    except lucene.JavaError as exc:  # pragma: no cover
        if not exception.instance_(exc.getJavaException()):
            raise


def convert(value):
    """Return python object from java Object."""
    if util.BytesRef.instance_(value):
        return util.BytesRef.cast_(value).utf8ToString()
    if not Number.instance_(value):
        return str(value) if Object.instance_(value) else value
    value = Number.cast_(value)
    if Float.instance_(value) or Double.instance_(value):
        return value.doubleValue()
    return value.longValue()
