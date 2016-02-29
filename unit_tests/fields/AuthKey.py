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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Key()

    def test_init(self):
        self.base = Key()
        self.base.table = 'test'
        self.base.field_name = 'authkey'
        self.assertEqual(self.base.value, self.base.empty)
        self.assertIsNone(self.base.primary)
        self.assertIsNone(self.base.unique)
        self.assertIsNone(self.base.index)
        self.assertIsNone(self.base.default)
        self.assertIsNone(self.base.notNull)
        self.assertEqual(self.base.size, 256)
        self.assertEqual(
            self.base.keyspace, string.ascii_letters+string.digits+'/.#+')

    def test_additional_kwargs(self):
        self.base = Key(value="foo")
        self.assertEqual(self.base.value, "foo")
        self.base = Key(default='field')
        self.assertEqual(self.base.default, 'field')
        self.base = Key(keyspace=string.ascii_letters)
        self.assertEqual(self.base.keyspace, string.ascii_letters)
        self.base = Key(size=512)
        self.assertEqual(self.base.size, 512)

    def test_validate(self):
        self.base = Key(size=512, not_null=True)
        self.assertFalse(self.base.validate())
        self.base.new()
        self.assertTrue(self.base.validate())

    def test_new(self):
        self.base = Key()
        self.base.new()
        a = self.base.value
        self.base.new()
        b = self.base.value
        self.assertNotEqual(a, b)
        self.assertAlmostEqual(
            len(self.base.value),
            ceil(chars_in(self.base.size, self.base.keyspace)))

    def test___call__(self):
        a = self.base.generate()
        self.assertEqual(self.base(a), a)
        self.assertEqual(self.base.value, a)
        self.assertEqual(self.base(), a)

    def test_generate(self):
        a = self.base.generate()
        self.base.value
        b = self.base.generate()
        self.assertNotEqual(a, b)
        self.assertAlmostEqual(
            len(a), ceil(chars_in(self.base.size, self.base.keyspace)))
        self.assertAlmostEqual(
            len(self.base.generate(512)),
            ceil(chars_in(512, self.base.keyspace)))
        self.assertAlmostEqual(
            bits_in(len(self.base.generate(512)), self.base.keyspace),
            512,
            delta=6)


if __name__ == '__main__':
    # Unit test
    unittest.main()
