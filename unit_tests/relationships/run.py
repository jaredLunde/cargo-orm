#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')

from kola import config
from unit_tests.relationships.BaseRelationship import *
from unit_tests.relationships.ForeignKey import *
from unit_tests.relationships.Relationship import *
from unit_tests.relationships._get_obj_from_string import *

if __name__ == '__main__':
    # Unit test
    unittest.main()
