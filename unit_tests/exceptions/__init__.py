#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
path = os.path.dirname(os.path.abspath(__file__)).split('bloom-orm')[0] + \
    'bloom-orm'
sys.path.insert(0, path)

from kola import config
from unit_tests.exceptions.QueryError import *
from unit_tests.exceptions.ValidationError import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
