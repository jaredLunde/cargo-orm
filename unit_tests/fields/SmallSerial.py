#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo import safe
from cargo.fields import SmallSerial

from unit_tests.fields.Serial import TestSerial
from unit_tests import configure


class TestSmallSerial(TestSerial):
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
    orm = configure.SmallSerialModel()

    @property
    def base(self):
        return self.orm.serial

    def test_init_(self):
        base = SmallSerial()
        self.assertEqual(base.value, base.empty)
        self.assertTrue(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, 1)
        self.assertEqual(base.maxval, 32767)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'smallint')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestSmallSerial, verbosity=2, failfast=True)
