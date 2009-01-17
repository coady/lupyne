import unittest
import sys, os

# PEP 366 notwithstanding, relative imports still don't work
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from local import LocalTest
from remote import RemoteTest

if __name__ == '__main__':
    unittest.main()
