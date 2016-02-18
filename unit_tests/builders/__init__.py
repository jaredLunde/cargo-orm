#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
path = os.path.dirname(os.path.abspath(__file__)).split('bloom-orm')[0] + \
    'bloom-orm'
sys.path.insert(0, path)


from kola import config
from unit_tests.builders.TableMeta import *
from unit_tests.builders.FieldMeta import *
from unit_tests.builders.Modeller import *
from unit_tests.builders.Builder import *
from unit_tests.builders.create_extension import *
from unit_tests.builders.create_schema import *
from unit_tests.builders.create_sequence import *
from unit_tests.builders.create_function import *
from unit_tests.builders.create_enum_type import *
from unit_tests.builders.create_type import *
from unit_tests.builders.create_view import *
from unit_tests.builders.create_user import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
