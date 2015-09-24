"""
Common utilities with no outside dependencies.
"""

import abc
import collections
import contextlib
try:
    import simplejson as json
except ImportError:
    import json  # noqa


@contextlib.contextmanager
def suppress(*exceptions, **attrs):
    "From Python 3: extended to also match attributes, e.g., error codes."
    try:
        yield
    except exceptions as exc:
        if any(getattr(exc, name, value) != value for name, value in attrs.items()):
            raise


class Atomic(object):
    "Abstract base class to distinguish singleton values from other iterables."
    __metaclass__ = abc.ABCMeta

    @classmethod
    def __subclasshook__(cls, other):
        return not issubclass(other, collections.Iterable) or NotImplemented

Atomic.register(basestring)


class method(staticmethod):
    "From Python 3: return functions instead of unbound methods when accessed through the class."
    def __get__(self, instance, owner):
        return self.__func__ if instance is None else self.__func__.__get__(instance, owner)
