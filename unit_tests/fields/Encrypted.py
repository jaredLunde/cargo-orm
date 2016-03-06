#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

import psycopg2.extras
import psycopg2.extensions

from kola import config
from vital.debug import RandData, logg

from bloom.fields import *

from unit_tests.fields.Field import *


class TestEncrypted(TestField):
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
    base = Encrypted(Encrypted.generate_secret())

    def test_init(self, *args, **kwargs):
        self.base = Encrypted(Encrypted.generate_secret())
        self.base.table = 'test'
        self.base.field_name = 'smallint'
        self.assertEqual(self.base.value, self.base.empty)
        self.assertIsNone(self.base.primary)
        self.assertIsNone(self.base.unique)
        self.assertIsNone(self.base.index)
        self.assertIsNone(self.base.default)
        self.assertIsNone(self.base.not_null)
        with self.assertRaises(TypeError):
            self.base = Encrypted()
        key = Encrypted.generate_secret()
        self.base = Encrypted(key)
        self.assertEqual(self.base.secret, key)
        self.assertIsInstance(self.base.type, Text)

    def test___call__(self):
        key = Encrypted.generate_secret()
        self.base = Encrypted(key)
        for val in ['abc', '4', 'test']:
            self.base(val)
            self.assertIsInstance(self.base.value, str)
            self.assertIsInstance(self.base.value, str)
            self.assertEqual(
                self.base.type(self.base.decrypt(self.base.encrypted)),
                self.base(val))
            self.assertEqual(self.base(self.base.encrypted), self.base(val))

        key = Encrypted.generate_secret()
        self.base = Encrypted(key, type=Binary(), factory=AESBytesFactory)
        for val in [b'abc', b'4', b'test']:
            self.base(val)
            self.assertIsInstance(self.base.value,  bytes)
            self.assertIsInstance(self.base.value,
                                  bytes)
            self.assertEqual(
                self.base.type(self.base.decrypt(self.base.encrypted)),
                self.base(val))

        key = Encrypted.generate_secret()
        self.base = Encrypted(key, type=Int())
        for val in [4, 3, 2, 1]:
            self.base(val)
            self.assertIsInstance(self.base.value,  int)
            self.assertEqual(
                self.base.type(self.base.decrypt(self.base.encrypted)), val)
            self.assertEqual(
                self.base.type(self.base.decrypt(self.base.encrypted)),
                self.base(val))

        key = Encrypted.generate_secret()
        self.base = Encrypted(key, type=Float())
        for val in [4.0, 3.0, 2.0, 1.0]:
            self.base(val)
            self.assertIsInstance(self.base.value,  float)
            self.assertEqual(
                self.base.type(self.base.decrypt(self.base.encrypted)), val)
            self.assertEqual(
                self.base.type(self.base.decrypt(self.base.encrypted)),
                self.base(val))

    def test_encrypted(self):
        self.base = Encrypted(Encrypted.generate_secret(),
                              type=Array(cast=int))
        l = RandData(int).list(5)
        self.base(l)
        self.base.append(6)
        l.append(6)
        self.assertIsInstance(self.base.value, list)
        self.assertEqual(self.base.decrypt(self.base.encrypted), l)

        self.base = Encrypted(Encrypted.generate_secret(),
                              type=Array(cast=str, dimensions=2))
        l = [[1, 2, 3, 4], [1, 2, 3, 4]]
        self.base(l)
        self.assertIsInstance(self.base.value, list)
        self.assertEqual(self.base.decrypt(self.base.encrypted), l)

        self.base = Encrypted(Encrypted.generate_secret(),
                              type=JsonB())
        d = RandData(str).dict(2, 2)
        self.base(d)
        self.assertIsInstance(self.base.value, dict)
        self.assertDictEqual(self.base.decrypt(self.base.encrypted), d)

    def test_generate_secret(self):
        for size in (16, 24, 32):
            self.assertEqual(size, len(self.base.generate_secret(size)))
        with self.assertRaises(ValueError):
            self.base.generate_secret(21)

    def test_factory(self):
        class Factory(object):
            @staticmethod
            def encrypt(val, secret):
                return val

            @staticmethod
            def decrypt(val, secret):
                return val

        self.base = Encrypted(Encrypted.generate_secret(), factory=Factory)
        self.assertEqual(self.base.factory, Factory)
        self.base('test')
        self.assertEqual(self.base.value.encrypted, self.base.prefix + 'test')
        self.assertEqual(self.base(self.base.prefix + 'test'), 'test')

    def test_validate(self):
        self.base = Encrypted(Encrypted.generate_secret())
        self.base('test')
        self.base.type.validation = lambda x: False
        self.assertFalse(self.base.validate())
        self.assertEqual(self.base.validation_error, "Failed validation")
        self.base.type.validation = lambda x: True
        self.assertTrue(self.base.validate())
        self.base.type.validation = lambda x: \
            isinstance(x, self.base.type.__class__)
        self.assertTrue(self.base.validate())

        self.base = Encrypted(Encrypted.generate_secret(),
                              type=Text(minlen=5))
        self.base('test')
        self.assertFalse(self.base.validate())
        self.base('test5')
        self.assertTrue(self.base.validate())

    def test__set_value(self):
        return


if __name__ == '__main__':
    # Unit test
    unittest.main()
