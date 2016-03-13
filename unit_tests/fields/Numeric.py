#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import copy
import decimal
from bloom.fields import Decimal

from unit_tests.fields.BigInt import TestBigInt
from unit_tests import configure


Numeric = Decimal


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
    decimal_places: int() maximum digit precision
    '''
    @property
    def base(self):
        return self.orm.dec

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
        self.assertEqual(base.decimal_places, -1)
        self.assertEqual(base.digits, -1)

    def test_additional_kwargs(self):
        base = Numeric(decimal_places=7)
        self.assertEqual(base.decimal_places, 7)

    def test___call__(self):
        for val in (4.0, 4, '4', '4.0', decimal.Decimal(4.0), '$4.00'):
            self.base(val)
            self.assertEqual(self.base(), 4.0)
        for val in ([], dict(), set(), {}):
            with self.assertRaises((TypeError, ValueError)):
                self.base(val)
        base = Numeric(decimal_places=5)
        for val in (4.82342352352352, '4.82342352352352'):
            base(val)
            self.assertEqual(
                float(base()), round(4.82342352352352, base.decimal_places))

    def test_insert(self):
        self.base(10)
        val = getattr(self.orm.naked().insert(), self.base.field_name)
        self.assertEqual(val, 10.0)

    def test_select(self):
        self.base(10)
        self.orm.insert()
        self.assertEqual(
            getattr(self.orm.new().get(), self.base.field_name).value,
            self.base.value)

    def test_copy(self):
        fielda = self.base
        fieldb = self.base.copy()
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            if k not in {'_context', 'validator'}:
                self.assertEqual(getattr(fielda, k), getattr(fieldb, k))
            else:
                self.assertNotEqual(getattr(fielda, k), getattr(fieldb, k))
        self.assertEqual(fielda.table, fieldb.table)

        fielda = self.base
        fieldb = copy.copy(self.base)
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            if k not in {'_context', 'validator'}:
                self.assertEqual(getattr(fielda, k), getattr(fieldb, k))
            else:
                self.assertNotEqual(getattr(fielda, k), getattr(fieldb, k))
        self.assertEqual(fielda.table, fieldb.table)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestNumeric, failfast=True, verbosity=2)
