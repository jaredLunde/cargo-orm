#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

import psycopg2.extras
from vital.debug import RandData
from vital.sql.fields import JSON

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Char import *


class TestJSON(TestField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = JSON()
        rd = RandData(str).dict(4, 2)
        self.base(rd)
        self.assertIsInstance(self.base.real_value, psycopg2.extras.Json)
        self.assertNotEqual(self.base.value, self.base.real_value)

    def test_real_value(self):
        self.assertIsInstance(self.base.real_value, psycopg2.extras.Json)


if __name__ == '__main__':
    # Unit test
    unittest.main()
