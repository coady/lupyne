from __future__ import print_function
import unittest
import sys
import lucene

from .local import TestCase as local
from .remote import TestCase as remote
from .distributed import TestCase as distributed

class TestRunner(unittest.TextTestRunner):
    def run(self, test):
        if self.verbosity > 1:
            print('python:', sys.version.split()[0], 'lucene:', lucene.VERSION)
        return unittest.TextTestRunner.run(self, test)

if __name__ == '__main__':
    lucene.initVM(vmargs='-Djava.awt.headless=true')
    unittest.main(testRunner=TestRunner)
