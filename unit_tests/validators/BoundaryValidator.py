#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Int
from bloom.validators import BoundaryValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestBoundaryValidator(TestValidator):
    field = Int(minval=2, maxval=5, validator=BoundaryValidator)

    def setUp(self):
        self.field.maxval = 5
        self.field.minval = 2
        self.field.clear()

    def test_validate_empty(self):
        self.assertFalse(self.field.validate())
        self.field.minval = 0
        self.assertTrue(self.field.validate())

    def test_minval_violation(self):
        self.field(1)
        self.assertFalse(self.field.validate())
        self.assertEqual(BoundaryValidator.MINVAL_CODE,
                         self.field.validator.code)
        self.field(2)
        self.assertTrue(self.field.validate())

    def test_maxval_violation(self):
        self.field(6)
        self.assertFalse(self.field.validate())
        self.assertEqual(BoundaryValidator.MAXVAL_CODE,
                         self.field.validator.code)
        self.field(5)
        self.assertTrue(self.field.validate())

    def test_validate_none(self):
        self.field(None)
        self.assertFalse(self.field.validate())
        self.field.minval = 0
        self.assertTrue(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBoundaryValidator, failfast=True, verbosity=2)
