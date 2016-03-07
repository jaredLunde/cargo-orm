#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Field
from bloom.validators import NullValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestNullValidator(TestValidator):
    field = Field(not_null=True, validator=NullValidator)

    def setUp(self):
        self.field.not_null = True
        self.field.clear()

    def test_validate_empty(self):
        self.assertFalse(self.field.validate())
        self.field.not_null = False
        self.assertTrue(self.field.validate())

    def test_validate_string(self):
        self.field('foo')
        self.assertTrue(self.field.validate())
        self.field.not_null = False
        self.assertTrue(self.field.validate())

    def test_validate_none(self):
        self.field(None)
        self.assertFalse(self.field.validate())
        self.field.not_null = False
        self.assertTrue(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestNullValidator, failfast=True, verbosity=2)
