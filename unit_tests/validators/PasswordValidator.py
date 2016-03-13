#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.etc import passwords
from bloom.fields import Password
from bloom.fields.extras import *
from bloom.validators import PasswordValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestPasswordValidator(TestValidator):
    field = Password(minlen=2, maxlen=5, validator=PasswordValidator)

    def setUp(self):
        self.field.maxlen = 5
        self.field.minlen = 2
        self.field.not_null = False
        self.field.hasher = Argon2Hasher()
        self.field.blacklist = passwords.blacklist
        self.field.clear()

    def test_validate_empty(self):
        self.assertTrue(self.field.validate())
        self.field.not_null = True
        self.assertFalse(self.field.validate())
        self.field.minlen = 0
        self.assertFalse(self.field.validate())

    def test_minlen_violation(self):
        self.field('f')
        self.assertFalse(self.field.validate())
        self.assertEqual(PasswordValidator.MINLEN_CODE,
                         self.field.validator.code)
        self.field('fo')
        self.assertTrue(self.field.validate())

    def test_maxlen_violation(self):
        self.field('abcbwe')
        self.assertFalse(self.field.validate())
        self.assertEqual(PasswordValidator.MAXLEN_CODE,
                         self.field.validator.code)

        self.field('abcbw')
        self.assertTrue(self.field.validate())

    def test_blacklist_violation(self):
        self.field.hasher = SHA256Hasher()
        self.field.hasher.rounds = 2500
        self.field.maxlen = -1
        for p in passwords.blacklist:
            self.field(p)
            self.assertFalse(self.field.validate())
            self.assertEqual(PasswordValidator.BLACKLISTED_CODE,
                             self.field.validator.code)
        self.field.blacklist = {}
        for p in passwords.blacklist:
            self.field(p)
            self.assertTrue(self.field.validate())

    def test_hash_violation(self):
        self.field.value = 'foo'
        self.field.validation_value = 'foo'
        self.assertFalse(self.field.validate())
        self.assertEqual(self.field.validator.code,
                         self.field.validator.HASHING_CODE)

    def test_validate_none(self):
        self.field(None)
        self.assertTrue(self.field.validate())
        self.field.minlen = 0
        self.assertTrue(self.field.validate())

        self.field.not_null = True
        self.field(None)
        self.assertFalse(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestPasswordValidator, failfast=True, verbosity=2)
