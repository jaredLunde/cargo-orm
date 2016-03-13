#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
path = os.path.dirname(os.path.abspath(__file__)).split('bloom-orm')[0] + \
    'bloom-orm'
sys.path.insert(0, path)

from unit_tests.statements.DELETE import *
from unit_tests.statements.INSERT import *
from unit_tests.statements.Intersections import *
from unit_tests.statements.RAW import *
from unit_tests.statements.SELECT import *
from unit_tests.statements.UPDATE import *
from unit_tests.statements.WITH import *

if __name__ == '__main__':
    # Unit test
    unittest.main()
