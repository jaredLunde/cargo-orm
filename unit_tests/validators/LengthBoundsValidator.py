#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import Char
from cargo.validators import LengthBoundsValidator

from unit_tests import configure
from unit_tests.validators.Validator import TestValidator


class TestLengthBoundsValidator(TestValidator):
    field = Char(minlen=2, maxlen=5, validator=LengthBoundsValidator)

    def setUp(self):
        self.field.maxlen = 5
        self.field.minlen = 2
        self.field.clear()

    def test_validate_empty(self):
        self.assertFalse(self.field.validate())
        self.field.minlen = 0
        self.assertTrue(self.field.validate())

    def test_validate_string(self):
        self.field('foo')
        self.assertTrue(self.field.validate())

    def test_minlen_violation(self):
        self.field('f')
        self.assertFalse(self.field.validate())
        self.assertEqual(LengthBoundsValidator.MINLEN_CODE,
                         self.field.validator.code)

    def test_maxlen_violation(self):
        self.field('abcbwe')
        self.assertFalse(self.field.validate())
        self.assertEqual(LengthBoundsValidator.MAXLEN_CODE,
                         self.field.validator.code)

    def test_validate_none(self):
        self.field(None)
        self.assertFalse(self.field.validate())
        self.field.minlen = 0
        self.assertTrue(self.field.validate())


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestLengthBoundsValidator, failfast=True, verbosity=2)
