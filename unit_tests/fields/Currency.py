#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Currency

from unit_tests.fields.Numeric import TestNumeric
from unit_tests import configure


class TestCurrency(TestNumeric):
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
    decimal_places: int() maximum digit precision
    '''
    @property
    def base(self):
        return self.orm.currency

    def test_init_(self):
        base = Currency()
        self.assertEqual(base.value, base.empty)
        self.assertIsNone(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, -92233720368547758.08)
        self.assertEqual(base.maxval, 92233720368547758.07)
        print(9223372036854775808.08)
        self.assertEqual(base.decimal_places, 2)

    def test_format(self):
        self.base('91,000.0')

if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestCurrency, failfast=True, verbosity=2)
