#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')

from kola import config
from unit_tests.statements.BaseQuery import *
from unit_tests.statements.DELETE import *
from unit_tests.statements.EXCEPT import *
from unit_tests.statements.INSERT import *
from unit_tests.statements.INTERSECT import *
from unit_tests.statements.Intersections import *
from unit_tests.statements.Query import *
from unit_tests.statements.RAW import *
from unit_tests.statements.SELECT import *
from unit_tests.statements.UNION import *
from unit_tests.statements.UPDATE import *
from unit_tests.statements.WITH import *

if __name__ == '__main__':
    # Unit test
    unittest.main()
