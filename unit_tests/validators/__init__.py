#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')


from vital import config
from unit_tests.sql.validators.Validate import *
from unit_tests.sql.validators.ValidationValue import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
