import unittest
import os, sys
import lucene

if __package__ == '':   # support testing in-place
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from local import TestCase as local
from remote import TestCase as remote
from distributed import TestCase as distributed

class TestRunner(unittest.TextTestRunner):
    def run(self, test):
        if self.verbosity > 1:
            print 'lucene version', lucene.VERSION
        return unittest.TextTestRunner.run(self, test)

if __name__ == '__main__':
    lucene.initVM(lucene.CLASSPATH)
    unittest.main(testRunner=TestRunner)
