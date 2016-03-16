#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import Field
from cargo.validators import TypeValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


StrValidator = TypeValidator
StrValidator.types = str


class TestTypeValidator(TestValidator):
    field = Field(validator=StrValidator)

    def test_validate_empty(self):
        self.assertFalse(self.field.validate())

    def test_validate_string(self):
        self.field('foo')
        self.assertTrue(self.field.validate())

    def test_validate_none(self):
        self.field(None)
        self.assertFalse(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestTypeValidator, failfast=True, verbosity=2)
