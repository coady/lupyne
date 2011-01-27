from __future__ import print_function
import unittest
import os, sys
import lucene

if __package__ == '':   # support testing in-place
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from local import TestCase as local
from remote import TestCase as remote
from distributed import TestCase as distributed

class TestRunner(unittest.TextTestRunner):
    def run(self, test):
        if self.verbosity > 1:
            print('python:', sys.version.split()[0], 'lucene:', lucene.VERSION)
        return unittest.TextTestRunner.run(self, test)

if __name__ == '__main__':
    lucene.initVM()
    unittest.main(testRunner=TestRunner)
