#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import Enum

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
        enum = [1, 2, 3, 4, 'five', 'six', 'seven']
        base = Enum(*enum)
        base.value = 5
        self.assertFalse(base.validate())
        base(None)
        self.assertTrue(base.validate())
        base('five')
        self.assertTrue(base.validate())

    def test___call__(self):
        enum = [1, 2, 3, 4, 'five', 'six', 'seven']
        base = Enum(*enum)
        self.assertTupleEqual(base.types, tuple(enum))
        for val in enum:
            self.assertEqual(base(val), val)
        for val in ['taco', {'b': 'c'}, [2], 1234, 6.0]:
            with self.assertRaises(ValueError):
                base(val)

    def test_insert(self):
        self.base('red')
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertEqual(val, 'red')

        self.base(None)
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertEqual(val, None)

    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base('blue')
        self.orm.insert(self.base)
        orm = self.orm.new()
        val = getattr(orm.naked().order_by(self.orm.uid.desc()).get(),
                      self.base.field_name)
        self.assertEqual(val, self.base.value)

    def test_array_insert(self):
        arr = ['red', 'white']
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val, arr)

    def test_array_select(self):
        arr = ['red', 'white']
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'enum_enum_type')
        self.assertEqual(self.base_array.type_name, 'array_enum_enum_type[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestEnum, failfast=True, verbosity=2)
