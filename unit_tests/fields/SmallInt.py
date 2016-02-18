#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from kola import config

from bloom.fields import SmallInt

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.fields.Field import *


class TestSmallInt(TestField):
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = SmallInt()
        self.base.table = 'test'
        self.base.field_name = 'smallint'
        self.assertIsNone(self.base.value)
        self.assertIsNone(self.base.primary)
        self.assertIsNone(self.base.unique)
        self.assertIsNone(self.base.index)
        self.assertIsNone(self.base.default)
        self.assertIsNone(self.base.notNull)
        self.assertEqual(self.base.minval, -32768)
        self.assertEqual(self.base.maxval, 32767)

    def test_additional_kwargs(self):
        self.base = SmallInt(minval=3)
        self.assertEqual(self.base.minval, 3)
        self.base = SmallInt(maxval=4)
        self.assertEqual(self.base.maxval, 4)
        self.base = SmallInt()

    def test_validate(self):
        self.base = SmallInt(minval=4, maxval=10)
        self.base(4)
        self.assertTrue(self.base.validate())
        self.base(3)
        self.assertFalse(self.base.validate())
        self.base(10)
        self.assertTrue(self.base.validate())
        self.base(11)
        self.assertFalse(self.base.validate())

    def test___call__(self):
        for error in ['abc', [], tuple(), set(), dict(), '4.2']:
            with self.assertRaises((ValueError, TypeError)):
                self.base(error)
        for num in [4.0, 4, 4.1, 4.9, '4']:
            self.base(num)
            self.assertEqual(self.base.value, 4)



if __name__ == '__main__':
    # Unit test
    unittest.main()
