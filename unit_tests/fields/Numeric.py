#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from vital import config

from vital.sql.fields import Numeric

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.BigInt import TestBigInt


class TestNumeric(TestBigInt):
    '''
    value: value to populate the field with
    not_null: bool() True if the field cannot be Null
    primary: bool() True if this field is the primary key in your table
    unique: bool() True if this field is a unique index in your table
    index: bool() True if this field is a plain index in your table, that is,
        not unique or primary
    default: default value to set the field to
    validation: callable() custom validation plugin, must return True if the
        field validates, and False if it does not
    digits: int() maximum digit precision
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Numeric()
        self.base.table = 'test'
        self.base.field_name = 'numeric'
        self.assertIsNone(self.base.value)
        self.assertIsNone(self.base.primary)
        self.assertIsNone(self.base.unique)
        self.assertIsNone(self.base.index)
        self.assertIsNone(self.base.default)
        self.assertIsNone(self.base.notNull)
        self.assertEqual(self.base.minval, -9223372036854775808.0)
        self.assertEqual(self.base.maxval, 9223372036854775807.0)
        self.assertEqual(self.base.digits, 15)

    def test_additional_kwargs(self):
        self.base = Numeric(digits=7)
        self.assertEqual(self.base.digits, 7)

    def test___call__(self):
        for val in (4.0, 4, '4', '4.0'):
            self.base(val)
            self.assertEqual(self.base(), 4.0)
        for val in ([], dict(), set(), {}):
            with self.assertRaises((TypeError, ValueError)):
                self.base(val)
        for val in (4.82342352352352, '4.82342352352352'):
            self.base(val)
            self.assertEqual(
                self.base(), round(4.82342352352352, self.base.digits))


if __name__ == '__main__':
    # Unit test
    unittest.main()
