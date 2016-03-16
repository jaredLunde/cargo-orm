#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import BigSerial
from unit_tests.fields.Serial import TestSerial
from unit_tests import configure


class TestBigSerial(TestSerial):
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
    orm = configure.BigSerialModel()

    @property
    def base(self):
        return self.orm.serial

    def test_init_(self):
        base = BigSerial()
        base.table = 'test'
        base.field_name = 'int'
        self.assertEqual(base.value, base.empty)
        self.assertTrue(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, 1)
        self.assertEqual(base.maxval, 9223372036854775807)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'bigint')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBigSerial, verbosity=2, failfast=True)
