#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Varbit
from bloom.validators import VarLengthValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestVarLengthValidator(TestValidator):
    field = Varbit(5, validator=VarLengthValidator)

    def setUp(self):
        self.field.length = 5
        self.field.clear()

    def test_validate_empty(self):
        self.assertTrue(self.field.validate())
        self.field.length = 0
        self.assertTrue(self.field.validate())

    def test_length_violation(self):
        self.field('0b001010')
        self.assertFalse(self.field.validate())
        self.assertEqual(VarLengthValidator.LENGTH_CODE,
                         self.field.validator.code)

        self.field('0b00101')
        self.field.validate()
        self.assertTrue(self.field.validate())

    def test_validate_none(self):
        self.field(None)
        self.assertTrue(self.field.validate())
        self.field.length = 0
        self.assertTrue(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestVarLengthValidator, failfast=True, verbosity=2)
