#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
path = os.path.dirname(os.path.abspath(__file__)).split('cargo-orm')[0] + \
    'cargo-orm'
sys.path.insert(0, path)


from kola import config
from unit_tests.relationships.BaseRelationship import *
from unit_tests.relationships.ForeignKey import *
from unit_tests.relationships.Relationship import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
