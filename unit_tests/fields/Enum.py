#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from docr import Docr
from bloom.fields import Enum

from unit_tests.fields.Char import *


class TestEnum(TestField):
    base = Enum([1, 2, 3, 4, 'five', 'six', 'seven'])

    def test_init(self):
        with self.assertRaises(TypeError):
            self.base = Enum()

    def test_validate(self):
        self.base = Enum([1, 2, 3, 4, 'five', 'six', 'seven'])
        self.base.value = 5
        self.assertFalse(self.base.validate())
        self.base(None)
        self.assertTrue(self.base.validate())
        self.base('five')
        self.assertTrue(self.base.validate())

    def test___call__(self):
        enum = [1, 2, 3, 4, 'five', 'six', 'seven']
        self.base = Enum([1, 2, 3, 4, 'five', 'six', 'seven'])
        self.assertTupleEqual(self.base.types, tuple(enum))
        for val in enum:
            self.assertEqual(self.base(val), val)
        for val in ['taco', {'b':'c'}, [2], 1234, 6.0]:
            with self.assertRaises(ValueError):
                self.base(val)


if __name__ == '__main__':
    # Unit test
    unittest.main()
