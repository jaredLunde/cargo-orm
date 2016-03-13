#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Enum

from unit_tests.fields.Field import *
from unit_tests import configure


class TestEnum(configure.SequenceTestCase, TestField):

    @property
    def base(self):
        return self.orm.enum

    def test_init(self):
        with self.assertRaises(TypeError):
            Enum()

    def test_validate(self):
        base = Enum(*[1, 2, 3, 4, 'five', 'six', 'seven'])
        base.value = 5
        self.assertFalse(base.validate())
        base(None)
        self.assertTrue(base.validate())
        base('five')
        self.assertTrue(base.validate())

    def test___call__(self):
        enum = [1, 2, 3, 4, 'five', 'six', 'seven']
        base = Enum(*[1, 2, 3, 4, 'five', 'six', 'seven'])
        self.assertTupleEqual(base.types, tuple(enum))
        for val in enum:
            self.assertEqual(base(val), val)
        for val in ['taco', {'b':'c'}, [2], 1234, 6.0]:
            with self.assertRaises(ValueError):
                base(val)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestEnum, failfast=True, verbosity=2)
