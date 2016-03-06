#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Numeric

from unit_tests.fields.BigInt import TestBigInt
from unit_tests import configure


class TestNumeric(configure.NumTestCase, TestBigInt):
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
        return self.orm.num

    def test_init_(self):
        base = Numeric()
        self.assertEqual(base.value, base.empty)
        self.assertIsNone(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, -9223372036854775808.0)
        self.assertEqual(base.maxval, 9223372036854775807.0)
        self.assertEqual(base.digits, -1)

    def test_additional_kwargs(self):
        base = Numeric(digits=7)
        self.assertEqual(base.digits, 7)

    def test___call__(self):
        for val in (4.0, 4, '4', '4.0'):
            self.base(val)
            self.assertEqual(self.base(), 4.0)
        for val in ([], dict(), set(), {}):
            with self.assertRaises((TypeError, ValueError)):
                self.base(val)
        self.base.digits = 5
        for val in (4.82342352352352, '4.82342352352352'):
            self.base(val)
            self.assertEqual(
                self.base(), round(4.82342352352352, self.base.digits))

    def test_insert(self):
        self.base(10)
        self.orm.insert()

    def test_select(self):
        self.base(10)
        self.orm.insert()
        self.assertEqual(
            getattr(self.orm.new().get(), self.base.field_name).value,
            self.base.value)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestNumeric, failfast=True, verbosity=2)
