#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

import psycopg2.extras
from vital.debug import RandData
from bloom.fields import JsonB

from unit_tests.fields.Char import *


class TestJsonB(TestField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = JsonB()

    def test_init_(self):
        self.base = JsonB()
        rd = RandData(str).dict(4, 2)
        self.base(rd)
        self.assertIsInstance(self.base.real_value, psycopg2.extras.Json)
        self.assertNotEqual(self.base.value, self.base.real_value)

    def test_cast(self):
        j = JsonB(cast=dict)
        with self.assertRaises(ValueError):
            j(['test'])
        j([('foo', 'bar')])
        self.assertIsInstance(j.value, dict)
        self.assertEqual(j['foo'], 'bar')

        j = JsonB(cast=list)
        j(['foo', 'bar'])
        self.assertEqual(j[0], 'foo')
        self.assertIsInstance(j.value, list)

    def test_real_value(self):
        self.base(RandData(str).dict(4, 2))
        self.assertIsInstance(self.base.real_value, psycopg2.extras.Json)


if __name__ == '__main__':
    # Unit test
    unittest.main()
