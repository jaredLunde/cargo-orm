#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Double

from unit_tests.fields.Numeric import TestNumeric
from unit_tests import configure


class TestDouble(TestNumeric):
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
    @property
    def base(self):
        return self.orm.float4

    def test_init_(self):
        base = Double()
        self.assertEqual(base.value, base.empty)
        self.assertIsNone(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, -9223372036854775808.0)
        self.assertEqual(base.maxval, 9223372036854775807.0)
        self.assertEqual(base.digits, 15)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestDouble, failfast=True, verbosity=2)
