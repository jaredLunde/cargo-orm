#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from kola import config

from bloom.fields import StrUID

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Int import TestInt


class TestStrUID(TestInt):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        super().__init__(*args, **kwargs)
        self.base = StrUID()
        self.base.table = 'test'
        self.base.field_name = 'uid'
        self.assertIsNone(self.base.value)
        self.assertTrue(self.base.primary)
        self.assertIsNone(self.base.unique)
        self.assertIsNone(self.base.index)
        self.assertIsNone(self.base.default)
        self.assertIsNone(self.base.notNull)
        self.assertEqual(self.base.minval, 1)
        self.assertEqual(self.base.maxval, 9223372036854775807)

    def test___call__(self):
        for check in [2223372036854775807, '2223372036854775808']:
            self.base(check)
            self.assertNotEqual(self.base(), int(check))
            self.assertEqual(self.base.value, int(check))
            self.assertNotEqual(str(self.base), int(self.base))
            self.assertEqual(str(self.base), self.base())
            self.assertEqual(int(self.base), self.base.value)
        self.assertEqual(self.base(12345678), self.base('12345678'))


if __name__ == '__main__':
    # Unit test
    unittest.main()
