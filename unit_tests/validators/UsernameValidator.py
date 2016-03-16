#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import Username
from cargo.etc.usernames import reserved_usernames
from cargo.validators import UsernameValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestUsernameValidator(TestValidator):
    field = Username(minlen=2, maxlen=5, validator=UsernameValidator)

    def setUp(self):
        self.field.maxlen = 5
        self.field.minlen = 2
        self.field.not_null = False
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
        self.assertEqual(UsernameValidator.MINLEN_CODE,
                         self.field.validator.code)
        self.field('xx')
        self.assertTrue(self.field.validate())

    def test_maxlen_violation(self):
        self.field('abcbwe')
        self.assertFalse(self.field.validate())
        self.assertEqual(UsernameValidator.MAXLEN_CODE,
                         self.field.validator.code)

        self.field('abcbw')
        self.assertTrue(self.field.validate())

    def test_char_validation(self):
        self.field('f@o')
        self.assertFalse(self.field.validate())
        self.field('foo')
        self.assertTrue(self.field.validate())

        base = 'foo'
        for c in '!@#$%^&*()œ∑´®†¥¨ˆøπåß∆˚¬…˜µ≤Ω-=+≈ç√':
            self.field(base + c)
            self.assertFalse(self.field.validate())
            self.assertEqual(self.field.validator.code,
                             self.field.validator.FORMAT_CODE)

    def test_reserved_validation(self):
        self.field.maxlen = 100
        for name in reserved_usernames:
            self.field(name)
            self.assertFalse(self.field.validate())
            self.assertEqual(self.field.validator.code,
                             self.field.validator.RESERVED_CODE)

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
    configure.run_tests(TestUsernameValidator, failfast=True, verbosity=2)
