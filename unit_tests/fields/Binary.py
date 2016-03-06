#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import psycopg2.extensions

from bloom.fields import Binary

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestBinary(configure.BinaryTestCase, TestField):
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
        return self.orm.binary_field

    def test___call__(self):
        for val in ['abc', 4, '4', b'test']:
            self.base(val)
            self.assertIsInstance(self.base.value, bytes)

    def test_value(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base(b'foo')
        self.assertIsInstance(self.base.value, bytes)
        self.base(None)
        self.assertIsNone(self.base.value)

    def test_insert(self):
        self.base(b'foo')
        self.orm.insert(self.base)

    def test_select(self):
        import pickle
        enc_val = Binary('foo')
        self.base(pickle.dumps(enc_val))
        self.orm.insert(self.base)
        r = self.orm.new().desc(self.orm.uid).get()
        r_val = getattr(r, self.base.field_name).value
        self.assertEqual(r_val, self.base.value)
        p_val = pickle.loads(r_val)
        self.assertIsInstance(p_val, enc_val.__class__)
        self.assertEqual(p_val.value, enc_val.value)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBinary, verbosity=2, failfast=True)
