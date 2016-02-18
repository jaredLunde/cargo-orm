#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')


from vital import config
from unit_tests.sql.builders.TableMeta import *
from unit_tests.sql.builders.FieldMeta import *
from unit_tests.sql.builders.Modeller import *
from unit_tests.sql.builders.Builder import *
from unit_tests.sql.builders.create_extension import *
from unit_tests.sql.builders.create_schema import *
from unit_tests.sql.builders.create_sequence import *
from unit_tests.sql.builders.create_function import *
from unit_tests.sql.builders.create_enum_type import *
from unit_tests.sql.builders.create_type import *
from unit_tests.sql.builders.create_view import *
from unit_tests.sql.builders.create_user import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
