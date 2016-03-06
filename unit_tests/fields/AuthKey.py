#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import string
import unittest

from kola import config

from bloom.fields import Key
from vital.security import *

from unit_tests.fields.Field import *


class TestKey(TestField):
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
    size: int() size of random bits to generate
    chars: iterable chars to include in the key
    '''
    def test_init(self):
        base = Key()
        base.table = 'test'
        base.field_name = 'authkey'
        self.assertEqual(base.value, base.empty)
        self.assertIsNone(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.size, 256)
        self.assertEqual(
            base.keyspace, string.ascii_letters+string.digits+'/.#+')

    def test_additional_kwargs(self):
        base = Key(value="foo")
        self.assertEqual(base.value, "foo")
        base = Key(default='field')
        self.assertEqual(base.default, 'field')
        base = Key(keyspace=string.ascii_letters)
        self.assertEqual(base.keyspace, string.ascii_letters)
        base = Key(size=512)
        self.assertEqual(base.size, 512)

    def test_validate(self):
        base = Key(size=512, not_null=True)
        self.assertFalse(base.validate())
        base.new()
        self.assertTrue(base.validate())

    def test_new(self):
        base = Key()
        base.new()
        a = base.value
        base.new()
        b = base.value
        self.assertNotEqual(a, b)
        self.assertAlmostEqual(
            len(base.value),
            ceil(chars_in(base.size, base.keyspace)))

    def test___call__(self):
        base = Key()
        a = base.generate()
        self.assertEqual(base(a), a)
        self.assertEqual(base.value, a)
        self.assertEqual(base(), a)

    def test_generate(self):
        base = Key()
        a = base.generate()
        base.value
        b = base.generate()
        self.assertNotEqual(a, b)
        self.assertAlmostEqual(
            len(a), ceil(chars_in(base.size, base.keyspace)))
        self.assertAlmostEqual(
            len(base.generate(512)),
            ceil(chars_in(512, base.keyspace)))
        self.assertAlmostEqual(
            bits_in(len(base.generate(512)), base.keyspace),
            512,
            delta=6)


if __name__ == '__main__':
    # Unit test
    unittest.main()
