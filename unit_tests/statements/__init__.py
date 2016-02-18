#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')


from kola import config
from unit_tests.sql.statements.BaseQuery import *
from unit_tests.sql.statements.DELETE import *
from unit_tests.sql.statements.INSERT import *
from unit_tests.sql.statements.Intersections import *
from unit_tests.sql.statements.Query import *
from unit_tests.sql.statements.RAW import *
from unit_tests.sql.statements.SELECT import *
from unit_tests.sql.statements.UPDATE import *
from unit_tests.sql.statements.WITH import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
