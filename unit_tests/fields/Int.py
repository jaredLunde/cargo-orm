#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
from cargo.fields import Int

from unit_tests import configure
from unit_tests.fields.SmallInt import TestSmallInt, TestEncSmallInt


__all__ = ("TestInt",)


class TestInt(TestSmallInt):
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
        base = Int()
        self.assertEqual(base.value, base.empty)
        self.assertIsNone(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, -2147483648)
        self.assertEqual(base.maxval, 2147483647)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'integer')
        self.assertEqual(self.base_array.type_name, 'integer[]')


class TestEncInt(TestInt, TestEncSmallInt):

    @property
    def base(self):
        return self.orm.enc_integer

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestInt, TestEncInt, verbosity=2, failfast=True)
