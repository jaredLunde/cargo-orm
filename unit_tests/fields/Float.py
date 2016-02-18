#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from kola import config

from vital.docr import Docr
from bloom.fields import Float
from bloom import create_pool

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Numeric import TestNumeric


class TestFloat(TestNumeric):

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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Float()
        self.base.table = 'test'
        self.base.field_name = 'decimal'
        self.assertIsNone(self.base.value)
        self.assertIsNone(self.base.primary)
        self.assertIsNone(self.base.unique)
        self.assertIsNone(self.base.index)
        self.assertIsNone(self.base.default)
        self.assertIsNone(self.base.notNull)
        self.assertEqual(self.base.minval, -9223372036854775808.0)
        self.assertEqual(self.base.maxval, 9223372036854775807.0)
        self.assertEqual(self.base.digits, 15)


if __name__ == '__main__':
    # Unit test
    unittest.main()
