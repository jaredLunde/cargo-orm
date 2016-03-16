#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import Bit
from cargo.validators import ExactLengthValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestExactLengthValidator(TestValidator):
    field = Bit(4, validator=ExactLengthValidator)

    def setUp(self):
        self.field.length = 4
        self.field.clear()

    def test_validate_empty(self):
        self.assertFalse(self.field.validate())
        self.field.length = 0
        self.assertTrue(self.field.validate())

    def test_length_violation(self):
        self.field('0b00101')
        self.assertFalse(self.field.validate())
        self.assertEqual(ExactLengthValidator.LENGTH_CODE,
                         self.field.validator.code)

    def test_validate_none(self):
        self.field(None)
        self.assertFalse(self.field.validate())
        self.field.length = 0
        self.assertTrue(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestExactLengthValidator, failfast=True, verbosity=2)
