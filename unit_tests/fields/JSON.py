#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

import psycopg2.extras
from vital.debug import RandData
from bloom.fields import Json

from unit_tests.fields.Char import *


class TestJson(TestField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Json()

    def test_init_(self):
        self.base = Json()
        rd = RandData(str).dict(4, 2)
        self.base(rd)
        self.assertIsInstance(self.base.real_value, psycopg2.extras.Json)
        self.assertNotEqual(self.base.value, self.base.real_value)

    def test_real_value(self):
        self.base(RandData(str).dict(4, 2))
        self.assertIsInstance(self.base.real_value, psycopg2.extras.Json)


if __name__ == '__main__':
    # Unit test
    unittest.main()
