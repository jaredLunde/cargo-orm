#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')

from kola import config
from unit_tests.expressions.ArrayItems import *
from unit_tests.expressions.BaseExpression import *
from unit_tests.expressions.BaseLogic import *
from unit_tests.expressions.BaseNumericLogic import *
from unit_tests.expressions.Case import *
from unit_tests.expressions.Clause import *
from unit_tests.expressions.DateLogic import *
from unit_tests.expressions.DateTimeLogic import *
from unit_tests.expressions.Expression import *
from unit_tests.expressions.Function import *
from unit_tests.expressions.Functions import *
from unit_tests.expressions.NumericLogic import *
from unit_tests.expressions.StringLogic import *
from unit_tests.expressions.Subquery import *
from unit_tests.expressions.TimeLogic import *
from unit_tests.expressions.alias import *

if __name__ == '__main__':
    # Unit test
    unittest.main()
