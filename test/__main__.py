import unittest
import sys, os

# append path to support testing in-place
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from local import TestCase as local
from remote import TestCase as remote

if __name__ == '__main__':
    unittest.main()
