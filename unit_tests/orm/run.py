#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')

from kola import config
from unit_tests.orm.Model import *
from unit_tests.orm.QueryState import *
from unit_tests.orm.RestModel import *
from unit_tests.orm.SQL import *

if __name__ == '__main__':
    # Unit test
    unittest.main()
