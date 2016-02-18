#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')

from vital import config
from unit_tests.sql.expressions.ArrayItems import *
from unit_tests.sql.expressions.BaseExpression import *
from unit_tests.sql.expressions.BaseLogic import *
from unit_tests.sql.expressions.BaseNumericLogic import *
from unit_tests.sql.expressions.Case import *
from unit_tests.sql.expressions.Clause import *
from unit_tests.sql.expressions.DateLogic import *
from unit_tests.sql.expressions.DateTimeLogic import *
from unit_tests.sql.expressions.Expression import *
from unit_tests.sql.expressions.Function import *
from unit_tests.sql.expressions.Functions import *
from unit_tests.sql.expressions.NumericLogic import *
from unit_tests.sql.expressions.StringLogic import *
from unit_tests.sql.expressions.Subquery import *
from unit_tests.sql.expressions.TimeLogic import *
from unit_tests.sql.expressions.alias import *

if __name__ == '__main__':
    # Unit test
    unittest.main()
