#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from kola import config

from bloom.fields import UID

from unit_tests.fields.Int import TestInt


class TestUID(TestInt):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = UID()
        self.base.table = 'test'
        self.base.field_name = 'uid'

    def test_init_(self):
        self.base = UID()
        self.assertEqual(self.base.value, self.base.empty)
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
            self.assertEqual(self.base(), int(check))


if __name__ == '__main__':
    # Unit test
    unittest.main()
