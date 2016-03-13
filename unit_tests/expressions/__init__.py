#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
path = os.path.dirname(os.path.abspath(__file__)).split('bloom-orm')[0] + \
    'bloom-orm'
sys.path.insert(0, path)

from unit_tests.expressions.BaseLogic import *
from unit_tests.expressions.Case import *
from unit_tests.expressions.Clause import *
from unit_tests.expressions.DateTimeLogic import *
from unit_tests.expressions.Expression import *
from unit_tests.expressions.Function import *
from unit_tests.expressions.NumericLogic import *
from unit_tests.expressions.StringLogic import *
from unit_tests.expressions.Subquery import *
from unit_tests.expressions.alias import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
