#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

import psycopg2.extras
from vital.debug import RandData
from bloom.fields import Json

from unit_tests.fields.Char import *


class TestJson(TestField):
    base = Json()

    def test_init_(self):
        self.base = Json()
        rd = RandData(str).dict(4, 2)
        self.base(rd)
        self.assertIsInstance(self.base.value, dict)
        self.base.clear()

    def test_real_value(self):
        self.base(RandData(str).dict(4, 2))
        self.assertIsInstance(self.base.value, dict)
        self.base(RandData(str).list(4, 2))
        self.assertIsInstance(self.base.value, list)
        self.base(RandData().randstr)
        self.assertIsInstance(self.base.value, str)
        self.base(RandData().randint)
        self.assertIsInstance(self.base.value, int)
        self.base.clear()


if __name__ == '__main__':
    # Unit test
    unittest.main()
