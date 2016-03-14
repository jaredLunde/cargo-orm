#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Double

from unit_tests.fields.Numeric import TestNumeric, TestEncNumeric
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
    decimal_places: int() maximum digit precision
    '''
    @property
    def base(self):
        return self.orm.float8

    def test_init_(self):
        base = Double()
        self.assertEqual(base.value, base.empty)
        self.assertIsNone(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, base.MINVAL)
        self.assertEqual(base.maxval, base.MAXVAL)
        self.assertEqual(base.decimal_places, 15)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'double precision')
        self.assertEqual(self.base_array.type_name, 'double precision[]')


class TestEncDouble(TestDouble, TestEncNumeric):

    @property
    def base(self):
        return self.orm.enc_float8

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestDouble, TestEncDouble, failfast=True, verbosity=2)
