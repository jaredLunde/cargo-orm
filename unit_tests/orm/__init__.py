#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
path = os.path.dirname(os.path.abspath(__file__)).split('bloom-orm')[0] + \
    'bloom-orm'
sys.path.insert(0, path)

from unit_tests.orm.Model import *
from unit_tests.orm.QueryState import *
from unit_tests.orm.RestModel import *
from unit_tests.orm.SQL import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
