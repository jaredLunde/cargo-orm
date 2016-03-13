#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Bool
from bloom.validators import BooleanValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestBooleanValidator(TestValidator):
    field = Bool(validator=BooleanValidator)

    def setUp(self):
        self.field.not_null = False
        self.field.clear()

    def test_validate_empty(self):
        self.assertTrue(self.field.validate())
        self.field.not_null = True
        self.assertFalse(self.field.validate())
        self.field(None)
        self.assertFalse(self.field.validate())

    def test_validate_in(self):
        self.field(True)
        self.assertTrue(self.field.validate())
        self.field(False)
        self.assertTrue(self.field.validate())

        self.field.not_null = True
        self.field(True)
        self.assertTrue(self.field.validate())
        self.field(False)
        self.assertTrue(self.field.validate())

    def test_validate_none(self):
        self.field(None)
        self.assertTrue(self.field.validate())
        self.field.not_null = True
        self.assertFalse(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBooleanValidator, failfast=True, verbosity=2)
