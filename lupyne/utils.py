"""
Common utilities with no outside dependencies.
"""

import abc
import collections
import contextlib
import types
try:
    import simplejson as json
except ImportError:
    import json  # noqa


@contextlib.contextmanager
def suppress(*exceptions):
    """Backport of contextlib.suppress."""
    try:
        yield
    except exceptions:  # pragma: no cover
        pass


class Atomic(object):
    """Abstract base class to distinguish singleton values from other iterables."""
    __metaclass__ = abc.ABCMeta

    @classmethod
    def __subclasshook__(cls, other):
        return not issubclass(other, collections.Iterable) or NotImplemented


Atomic.register(basestring)


class method(staticmethod):
    """Backport of Python 3's unbound methods."""
    def __get__(self, instance, owner):
        return self.__func__ if instance is None else types.MethodType(self.__func__, instance)
