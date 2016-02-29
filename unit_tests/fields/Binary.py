#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

import psycopg2.extensions

from kola import config

from bloom.fields import Binary

from unit_tests.fields.Field import *


class TestBinary(TestField):
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
    base = Binary()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Binary()
        self.base.table = 'test'
        self.base.field_name = 'bin'

    def test___call__(self):
        for val in ['abc', 4, '4', b'test']:
            self.base(val)
            self.assertIsInstance(self.base.value, bytes)
            self.assertIsInstance(self.base.real_value,
                                  psycopg2.extensions.Binary)



if __name__ == '__main__':
    # Unit test
    unittest.main()
