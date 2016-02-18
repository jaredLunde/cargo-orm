#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from vital.sql.fields import Char

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Field import *


class TestChar(TestField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Char()
        self.base.table = 'test'
        self.base.field_name = 'char'

    def test_validate(self):
        self.base.minlen, self.base.maxlen, self.base.notNull = 1, 2, True
        self.base('123')
        self.assertFalse(self.base.validate())
        self.base('12')
        self.assertTrue(self.base.validate())
        self.base('')
        self.assertFalse(self.base.validate())
        self.base = Char()

    def test___call__(self):
        for val in (40, [], dict(), set(), tuple(), 'foo', self):
            self.base(val)
            self.assertEqual(self.base.value, str(val))

    def test_additional_kwargs(self):
        char = Char(minlen=1)
        self.assertEqual(char.minlen, 1)
        char = Char(maxlen=5)
        self.assertEqual(char.maxlen, 5)


if __name__ == '__main__':
    # Unit test
    unittest.main()
