#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Char
from bloom.validators import CharValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestCharValidator(TestValidator):
    field = Char(minlen=2, maxlen=5, validator=CharValidator)

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
        self.assertEqual(CharValidator.MINLEN_CODE,
                         self.field.validator.code)
        self.field('fo')
        self.assertTrue(self.field.validate())

    def test_maxlen_violation(self):
        self.field('abcbwe')
        self.assertFalse(self.field.validate())
        self.assertEqual(CharValidator.MAXLEN_CODE,
                         self.field.validator.code)

        self.field('abcbw')
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
    configure.run_tests(TestCharValidator, failfast=True, verbosity=2)
