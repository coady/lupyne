import unittest
import os, sys
import lucene

if __package__ == '':   # support testing in-place
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from local import options, TestCase as local
from remote import TestCase as remote
from distributed import TestCase as distributed

if __name__ == '__main__':
    lucene.initVM(lucene.CLASSPATH)
    if options.verbose:
        print 'lucene version', lucene.VERSION
    unittest.main()
