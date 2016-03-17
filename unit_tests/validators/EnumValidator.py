#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import Enum
from cargo.validators import EnumValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestEnumValidator(TestValidator):
    field = Enum('bingo', 'bango', 'bongo', validator=EnumValidator)

    def setUp(self):
        self.field.not_null = False
        self.field.clear()

    def test_validate_empty(self):
        self.assertTrue(self.field.validate())
        self.field.not_null = True
        self.assertFalse(self.field.validate())

    def test_validate_in(self):
        self.field('bingo')
        self.assertTrue(self.field.validate())
        self.field.value = 'bengo'
        self.assertFalse(self.field.validate())

    def test_validate_none(self):
        self.field(None)
        self.assertTrue(self.field.validate())
        self.field.not_null = True
        self.assertFalse(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestEnumValidator, failfast=True, verbosity=2)
