#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import psycopg2.extensions
from bitstring import BitArray

from bloom.fields import Varbit
from bloom.validators import VarbitValidator

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestVarbit(configure.BitTestCase, TestField):
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
    '''
    @property
    def base(self):
        return self.orm.varbit_field

    def test_init(self, *args, **kwargs):
        base = self.base.__class__(4)
        self.assertEqual(self.base.length, 4)
        self.assertEqual(base.value, base.empty)
        base = self.base.__class__(4, primary=True)
        self.assertEqual(base.primary, True)
        base = self.base.__class__(4, unique=True)
        self.assertEqual(base.unique, True)
        base = self.base.__class__(4, index=True)
        self.assertEqual(base.index, True)
        base = self.base.__class__(4, default='field')
        self.assertEqual(base.default, 'field')
        base = self.base.__class__(4, not_null=True)
        self.assertEqual(base.not_null, True)
        base = self.base.__class__(4, validator=VarbitValidator)
        self.assertIsInstance(base.validator, VarbitValidator)

    def test___call__(self):
        for val in ['0b0010', BitArray(bin='0010')]:
            self.base(val)
            self.assertIsInstance(self.base.value, BitArray)
            self.assertEqual(len(self.base.value), 4)

    def test_value(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base('0b0010')
        self.assertIsInstance(self.base.value, BitArray)
        self.base(None)
        self.assertIsNone(self.base.value)

    def test_insert(self):
        self.base('0b0010')
        self.orm.insert(self.base)
        self.base('0b001')
        self.orm.insert(self.base)

    def test_select(self):
        self.base('0b0010')
        self.orm.insert()
        self.assertEqual(
            getattr(self.orm.new().get(), self.base.field_name).value,
            self.base.value)

    def test_array_insert(self):
        arr = ['0b0010', '0b0011']
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val, arr)

    def test_array_select(self):
        arr = ['0b0010', '0b0011']
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'varbit(4)')
        self.assertEqual(self.base_array.type_name, 'varbit(4)[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestVarbit, verbosity=2, failfast=True)
