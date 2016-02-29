#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from kola import config

from bloom.fields import BigInt

from unit_tests.fields.SmallInt import TestSmallInt


class TestBigInt(TestSmallInt):
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
    minval: int() minimum interger value
    maxval: int() maximum integer value
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = BigInt()

    def test_init_(self):
        self.base = BigInt()
        self.base.table = 'test'
        self.base.field_name = 'int'
        self.assertEqual(self.base.value, self.base.empty)
        self.assertIsNone(self.base.primary)
        self.assertIsNone(self.base.unique)
        self.assertIsNone(self.base.index)
        self.assertIsNone(self.base.default)
        self.assertIsNone(self.base.notNull)
        self.assertEqual(self.base.minval, -9223372036854775808)
        self.assertEqual(self.base.maxval, 9223372036854775807)


if __name__ == '__main__':
    # Unit test
    unittest.main()
