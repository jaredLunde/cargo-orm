#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from kola import config

from docr import Docr
from bloom.fields import Password
from bloom import create_pool, ValidationError
from vital.security import randkey

from unit_tests.fields.Char import *


class TestPassword(TestChar):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Password()
        self.base.table = 'test'
        self.base.field_name = 'password'

    def test_init_(self):
        self.base = Password()
        self.assertEqual(self.base.maxlen, -1)
        self.assertEqual(self.base.minlen, 8)
        self.assertEqual(self.base.salt_size, 16)
        self.assertEqual(self.base.strict, True)
        self.assertEqual(self.base.value, None)
        self.assertEqual(self.base.notNull, None)
        self.assertEqual(self.base.scheme, 'argon2')
        self.assertEqual(self.base.primary, None)
        self.assertEqual(self.base.unique, None)
        self.base(None)

    def test__should_insert(self):
        with self.assertRaises(ValidationError):
            self.base._should_insert()
        self.base('fingersInTheCookieJar')
        self.assertTrue(self.base._should_insert())
        self.base('finge')
        with self.assertRaises(ValidationError):
            self.base._should_insert()
        self.base(None)

    def test_validate(self):
        self.base.minlen, self.base.maxlen = 1, 2
        self.base('123')
        self.assertFalse(self.base.validate())
        self.base('12')
        self.assertTrue(self.base.validate())
        self.base('')
        self.assertFalse(self.base.validate())
        self.base = Password()
        self.assertFalse(self.base.validate())
        self.base('fingersInTheCookieJar')
        self.assertTrue(self.base.validate())
        self.base('finge')
        self.assertFalse(self.base.validate())
        self.base('password')
        self.assertFalse(self.base.validate())
        self.base(None)

    def test__should_update(self):
        with self.assertRaises(ValidationError):
            self.base._should_update()
        self.base('fingersInTheCookieJar')
        self.assertTrue(self.base._should_update())
        self.base('finge')
        with self.assertRaises(ValidationError):
            self.base._should_update()
        self.base(None)

    def test_generate(self):
        self.assertTrue(
            len(self.base.generate(128)) > len(self.base.generate(64)))
        self.base(None)

    def test_argon2(self):
        hash = self.base.argon2_encrypt('fish')
        self.assertTrue(self.base.argon2_verify('fish', hash))
        self.assertFalse(self.base.argon2_verify('fisher', hash))
        self.assertIn('argon2i', hash)
        self.base('fish')
        self.assertIn('argon2i', self.base.value)
        self.assertEqual('fish', self.base.validation_value)
        self.base(None)
        self.base.scheme = 'argon2'

    def test_encrypt(self):
        hash = self.base.encrypt('fish')
        self.assertTrue(self.base.verify('fish', hash))
        self.assertIn('argon2i', hash)
        self.base('fish')
        self.assertIn('argon2i', self.base.value)
        self.assertEqual('fish', self.base.validation_value)
        self.base.scheme = 'pbkdf2_sha512'
        hash = self.base.encrypt('fish')
        self.assertTrue(self.base.verify('fish', hash))
        self.assertNotIn('argon2i', hash)
        self.base(None)

    def test___call__(self):
        self.base('test')
        self.assertEqual(self.base.validation_value, 'test')
        self.assertTrue(self.base.is_hash(self.base.value))
        self.assertTrue(self.base.is_hash(self.base()))
        self.base(self.base())
        self.assertEqual(self.base.validation_value, 'test')
        self.assertTrue(self.base.is_hash(self.base.value))
        self.assertTrue(self.base.is_hash(self.base()))
        self.base(None)

    def test_verify(self):
        hash = self.base.encrypt('fish')
        self.assertTrue(self.base.verify('fish', hash))
        self.assertFalse(self.base.verify('fiserh', hash))
        self.base(None)

    def test_is_hash(self):
        self.assertTrue(self.base.is_hash(self.base('fish')))
        for scheme in self.base.schemes:
            self.base.rounds = 5
            self.base.scheme = scheme
            self.assertTrue(self.base.is_hash(self.base('fish')))
        for string in (randkey(128) for _ in range(1000)):
            self.base(string)
            self.assertFalse(self.base.is_hash(self.base.validation_value))
        self.assertFalse(self.base.is_hash(
            '$argon3i$m=3,t=1,p=1$aweflkaewgLWEGN$alwefawefweflawefewaf'))
        self.base.scheme = 'argon2'
        self.base.rounds = 12
        self.base(None)

    def test_additional_kwargs(self):
        pwd = Password(value='fish')
        self.assertEqual(pwd.validation_value, 'fish')
        schemes = ['pbkdf2_sha512']
        pwd = Password(schemes=schemes)
        self.assertEqual(pwd.schemes, schemes)
        pwd = Password(primary=True)
        self.assertEqual(pwd.primary, True)
        pwd = Password(unique=True)
        self.assertEqual(pwd.unique, True)
        pwd = Password(index=True)
        self.assertEqual(pwd.index, True)
        pwd = Password(default='password')
        self.assertEqual(pwd.default, 'password')
        pwd = Password(minlen=1)
        self.assertEqual(pwd.minlen, 1)
        pwd = Password(maxlen=5)
        self.assertEqual(pwd.maxlen, 5)



if __name__ == '__main__':
    # Unit test
    unittest.main()
