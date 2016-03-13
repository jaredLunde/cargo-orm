#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import unittest
import string
from math import ceil

from bloom.fields.extras import *
from bloom.exceptions import IncorrectPasswordError

from vital.security import chars_in, bits_in

from unit_tests.fields.Char import TestChar
from unit_tests import configure


class TestKey(configure.ExtrasTestCase, TestChar):

    @property
    def base(self):
        return self.orm.key

    def test_init(self):
        base = Key()
        base.table = 'test'
        base.field_name = 'key'
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

    def test_insert(self):
        self.base.new()
        self.assertIsNotNone(self.orm.naked().insert(self.base).key)

    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base.new()
        self.orm.insert(self.base)
        self.assertEqual(self.orm.new().desc(self.orm.uid).get().key.value,
                         self.base.value)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestKey, failfast=True, verbosity=2)
