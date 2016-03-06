#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
from bloom.fields import BigInt

from unit_tests.fields.SmallInt import TestSmallInt
from unit_tests import configure


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
    @property
    def base(self):
        return self.orm.integer

    def test_init_(self):
        base = BigInt()
        base.table = 'test'
        base.field_name = 'int'
        self.assertEqual(base.value, base.empty)
        self.assertIsNone(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, -9223372036854775808)
        self.assertEqual(base.maxval, 9223372036854775807)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBigInt, verbosity=2, failfast=True)
