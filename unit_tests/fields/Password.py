#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import unittest
from cargo.fields.extras import *
from cargo.exceptions import IncorrectPasswordError

from unit_tests.fields.Field import TestField
from unit_tests import configure


class FakeHasher(Hasher):
    scheme = 'fake_hasher'


class HasherTests(object):
    base = Hasher(raises=False)

    def test_register_scheme(self):
        self.base.register_scheme(FakeHasher)
        self.assertIs(self.base.find_class('fake_hasher'), FakeHasher)

    def test_verify(self):
        self.assertTrue(self.base.verify('foo', self.base.hash('foo')))
        self.assertFalse(self.base.verify('fooo', self.base.hash('foo')))

    def test_is_hash(self):
        self.assertTrue(self.base.is_hash(self.base.hash('foo')))
        self.assertFalse(self.base.is_hash('$sfwmef$alnaewlgew$alngawgwegg'))
        self.assertFalse(self.base.is_hash('$argon2d$alnaewlgew$alngawgwegg'))

    def test_identify(self):
        self.assertEqual(self.base.identify(self.base.hash('foo')),
                         self.base.scheme)
        self.assertIs(self.base.identify(self.base.hash('foo'),
                                         find_class=True),
                      self.base.__class__)

    def test_slots(self):
        self.assertFalse(hasattr(self.base, '__dict__'))

    def test_raises(self):
        self.base.raises = True
        with self.assertRaises(IncorrectPasswordError):
            self.base.verify('fooo', self.base.hash('foo'))
        self.assertTrue(self.base.verify('foo', self.base.hash('foo')))


class TestArgon2Hasher(unittest.TestCase, HasherTests):
    def setUp(self):
        self.base = self.base.__class__(raises=False)
    base = Argon2Hasher(raises=False)


class TestBcrypt256Hasher(TestArgon2Hasher):
    base = BcryptHasher(raises=False)


class TestBcryptHasher(TestArgon2Hasher):
    base = BcryptHasher(raises=False)


class TestPBKDF2Hasher(TestArgon2Hasher):
    base = PBKDF2Hasher(raises=False)


class TestSHA256Hasher(TestArgon2Hasher):
    base = SHA256Hasher(raises=False)


class TestSHA512Hasher(TestArgon2Hasher):
    base = SHA512Hasher(raises=False)


class TestPassword(configure.ExtrasTestCase, TestField):

    @property
    def base(self):
        self.orm.password.hasher.raises = False
        return self.orm.password

    def test_init(self):
        base = Password()
        self.assertEqual(base.maxlen, -1)
        self.assertEqual(base.minlen, 8)
        self.assertEqual(base.value, base.empty)
        self.assertIsNone(base.not_null)
        self.assertIsInstance(base.hasher, Argon2Hasher)
        self.assertIsNone(base.primary)
        self.assertIsNone(base.unique)

    def test___call__(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base('somepassword')
        self.assertEqual(self.base.validation_value, 'somepassword')
        self.assertTrue(self.base.hasher.is_hash(self.base.value))
        hsh = self.base.hash('somepassword')
        self.base(hsh)
        self.assertIsNone(self.base.validation_value)
        self.assertEqual(self.base.value, hsh)
        self.base(None)
        self.assertIsNone(self.base.value)
        self.assertIsNone(self.base.validation_value)
        self.base(self.base.empty)
        self.assertIsNone(self.base.value)
        self.base.clear()
        self.assertIs(self.base.value, self.base.empty)

    def test_clear(self):
        self.base('algwhglegaweg')
        self.assertIsNot(self.base.value, self.base.empty)
        self.assertIsNot(self.base.validation_value, self.base.empty)
        self.base.clear()
        self.assertIs(self.base.value, self.base.empty)
        self.assertIs(self.base.validation_value, self.base.empty)

    hashes = [Argon2Hasher, BcryptHasher, Bcrypt256Hasher, PBKDF2Hasher,
              SHA512Hasher, SHA256Hasher]

    def test_migrate(self):
        for h1, h2 in zip(self.hashes, reversed(self.hashes)):
            base = Password(h1(raises=False))
            hsh = base('somepassword')
            self.assertIsInstance(base._get_hasher_for(hsh), h1)
            self.assertTrue(base.verify('somepassword'))

            base.hasher = h2(raises=False)
            self.assertTrue(base.verify_and_migrate('somepassword'))
            self.assertIsInstance(base._get_hasher_for(base.value), h2)
            self.assertTrue(base.verify('somepassword'))

            hsh = base.value
            self.assertTrue(base.verify_and_migrate('somepassword'))
            self.assertIsInstance(base._get_hasher_for(base.value), h2)
            self.assertTrue(base.verify('somepassword'))
            self.assertEqual(base.value, hsh)

    def test_refresh(self):
        for h1, h2 in zip(self.hashes, reversed(self.hashes)):
            base = Password(h1(raises=False))
            hsh = base('somepassword')
            self.assertIsInstance(base._get_hasher_for(hsh), h1)
            self.assertTrue(base.verify('somepassword'))

            base.hasher = h2(raises=False)
            self.assertTrue(base.verify_and_refresh('somepassword'))
            self.assertIsInstance(base._get_hasher_for(base.value), h1)
            self.assertTrue(base.verify('somepassword'))
            self.assertNotEqual(base.value, hsh)

    def test_verify(self):
        self.assertFalse(self.base.verify('foo'))
        self.base('foo')
        self.assertTrue(self.base.verify('foo'))
        self.assertFalse(self.base.verify('fooo'))
        self.base('bar')
        self.assertTrue(self.base.verify('foo', self.base.hash('foo')))
        self.assertFalse(self.base.verify('fooo', self.base.hash('foo')))

    def test_verify_and_change(self):
        self.assertFalse(self.base.verify_and_change('foo', 'bar'))
        self.base('foo')
        self.assertIsNotNone(self.base.verify_and_change('foo', 'bar'))
        self.assertFalse(self.base.verify_and_change('baz', 'bar'))
        self.assertIsNotNone(self.base.verify_and_change('bar', 'baz'))

    def test_insert(self):
        self.base('somepassword')
        val = self.orm.new().insert()
        self.assertEqual(getattr(val, self.base.field_name).value,
                         self.base.value)

    def test_select(self):
        self.base('somepassword')
        self.orm.insert()
        val = self.orm.new().desc(self.orm.uid).get()
        self.assertEqual(getattr(val, self.base.field_name).value,
                         self.base.value)

    def test_array_insert(self):
        arr = ['somepassword', 'someotherpassword']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, self.base_array.value)

    def test_array_select(self):
        arr = ['somepassword', 'someotherpassword']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


class TestEncPassword(TestPassword):

    @property
    def base(self):
        self.orm.enc_password.hasher.raises = False
        return self.orm.enc_password

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')

    def test_deepcopy(self):
        pass


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestArgon2Hasher,
                        TestBcryptHasher,
                        TestBcrypt256Hasher,
                        TestPBKDF2Hasher,
                        TestSHA512Hasher,
                        TestSHA256Hasher,
                        TestPassword,
                        TestEncPassword,
                        failfast=True,
                        verbosity=2)
