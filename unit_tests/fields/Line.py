#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from collections import namedtuple

from bloom import Function
from bloom.builder import *
from bloom.fields import Line, UID
from bloom.fields.geometry import LineRecord

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestLine(configure.GeoTestCase, TestField):

    @property
    def base(self):
        return self.orm.line

    def test_init(self):
        self.orm.line.clear()
        self.assertEqual(self.orm.line.value, self.orm.line.empty)
        self.assertIsNone(self.orm.line.primary)
        self.assertIsNone(self.orm.line.unique)
        self.assertIsNone(self.orm.line.index)
        self.assertIsNone(self.orm.line.default)
        self.assertIsNone(self.orm.line.not_null)

    def test___call__(self):
        d = RandData(int).tuple(3)
        self.orm.line(d)
        self.assertIsInstance(self.orm.line.value, LineRecord)
        self.assertIsInstance(self.orm.line.a, int)
        self.assertIsInstance(self.orm.line.b, int)
        self.assertIsInstance(self.orm.line.c, int)
        with self.assertRaises(TypeError):
            self.orm.line(1234)

    def test_insert(self):
        d = RandData(int).tuple(3)
        self.orm.line(d)
        self.orm.insert()

    def test_select(self):
        d = (1, 2, 3)
        self.orm.line(d)
        self.orm.insert()
        self.assertSequenceEqual(
            self.orm.new().get().line.value,
            self.base.value)
        self.assertTupleEqual(
            tuple(self.orm.naked().get().line), tuple(d))

    def test_array_insert(self):
        arr = [(1, 2, 3), (4, 5, 6)]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val, self.base_array.value)

    def test_array_select(self):
        arr = [(1, 2, 3), (4, 5, 6)]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'line')
        self.assertEqual(self.base_array.type_name, 'line[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestLine, verbosity=2, failfast=True)
