#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from vital.sql.fields import Text

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Char import *


class TestText(TestChar):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Text()
        self.base.table = 'test'
        self.base.field_name = 'text'


if __name__ == '__main__':
    # Unit test
    unittest.main()
