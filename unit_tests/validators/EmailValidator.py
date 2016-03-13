#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Email
from bloom.validators import EmailValidator

from vital.debug import RandData

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestEmailValidator(TestValidator):
    field = Email(minlen=2, maxlen=5, validator=EmailValidator)

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
        self.assertEqual(EmailValidator.MINLEN_CODE,
                         self.field.validator.code)
        self.minlen = 4
        self.field('f@o.c')
        self.assertTrue(self.field.validate())

    def test_maxlen_violation(self):
        self.field('abcbwe')
        self.assertFalse(self.field.validate())
        self.assertEqual(EmailValidator.MAXLEN_CODE,
                         self.field.validator.code)
        self.field('a@b.c')
        self.field.validate()
        self.assertTrue(self.field.validate())

    def test_format_violation(self):
        self.field.maxlen = 240
        self.field('abcbwe')
        self.assertFalse(self.field.validate())
        self.assertEqual(EmailValidator.FORMAT_CODE,
                         self.field.validator.code)

        self.field('abcbw@blahblah.com')
        self.assertTrue(self.field.validate())

        self.field('abcbw@blahblah')
        self.assertFalse(self.field.validate())
        self.assertEqual(EmailValidator.FORMAT_CODE,
                         self.field.validator.code)

        for e in RandData(RandData.emailType).list(1000):
            self.field(e)
            self.assertTrue(self.field.validate())

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
    configure.run_tests(TestEmailValidator, failfast=True, verbosity=2)
