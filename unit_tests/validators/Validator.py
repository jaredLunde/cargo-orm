#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import unittest
from bloom.fields import Field
from bloom.validators import Validator

from unit_tests import configure


class TestValidator(unittest.TestCase):
    field = Field(not_null=True, validator=Validator)

    def setUp(self):
        self.field.clear()

    def test_is(self):
        self.assertIs(self.field.validator.field, self.field)

    def test_raise(self):
        with self.assertRaises(self.field.validator.raises):
            raise self.field.validator.raises(self.field.validator.error)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestValidator, failfast=True, verbosity=2)
