"""
Common utilities with no lucene dependencies.
"""

import abc
import bisect
import collections
import contextlib
import heapq
import itertools
import types
import six
try:
    import simplejson as json
except ImportError:
    import json  # noqa
long = int if six.PY3 else long  # noqa


@contextlib.contextmanager
def suppress(*exceptions):
    """Backport of contextlib.suppress."""
    try:
        yield
    except exceptions:  # pragma: no cover
        pass


class Atomic(six.with_metaclass(abc.ABCMeta)):
    """Abstract base class to distinguish singleton values from other iterables."""
    @classmethod
    def __subclasshook__(cls, other):
        return not issubclass(other, collections.Iterable) or NotImplemented


for string in six.string_types:
    Atomic.register(string)


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
        self.alphabet = sorted(set(itertools.chain.from_iterable(self.words)))
        self.suffix = self.alphabet[-1] * max(map(len, self.words)) if self.alphabet else ''
        self.prefixes = {word[:stop] for word in self.words for stop in range(len(word) + 1)}

    def suggest(self, prefix, count=None):
        """Return ordered suggested words for prefix."""
        start = bisect.bisect_left(self.words, prefix)
        stop = bisect.bisect_right(self.words, prefix + self.suffix, start)
        words = self.words[start:stop]
        if count is not None and count < len(words):
            return heapq.nlargest(count, words, key=self.__getitem__)
        words.sort(key=self.__getitem__, reverse=True)
        return words

    def edits(self, word, length=0):
        """Return set of potential words one edit distance away, mapped to valid prefix lengths."""
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
        """Generate ordered sets of words by increasing edit distance."""
        previous, edits = set(), {word: 0}
        for distance in range(len(word)):
            yield sorted(filter(self.__contains__, edits), key=self.__getitem__, reverse=True)
            previous.update(edits)
            groups = map(self.edits, edits, edits.values())
            edits = {edit: group[edit] for group in groups for edit in group if edit not in previous}
