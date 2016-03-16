#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from collections import namedtuple

from cargo import Function
from cargo.builder import *
from cargo.fields import Box, UID
from cargo.fields.geometry import BoxRecord

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestBox(configure.GeoTestCase, TestField):

    @property
    def base(self):
        return self.orm.box

    def test_init(self):
        self.assertEqual(self.orm.box.value, self.orm.box.empty)
        self.assertIsNone(self.orm.box.primary)
        self.assertIsNone(self.orm.box.unique)
        self.assertIsNone(self.orm.box.index)
        self.assertIsNone(self.orm.box.default)
        self.assertIsNone(self.orm.box.not_null)

    def test___call__(self):
        d = RandData(int).tuple(2, 2)
        self.orm.box(d)
        self.assertIsInstance(self.orm.box.value, BoxRecord)
        self.assertIsInstance(self.orm.box.a, tuple)
        self.assertIsInstance(self.orm.box.a.x, int)
        with self.assertRaises(TypeError):
            self.orm.box(1234)

    def test_insert(self):
        d = RandData(int).tuple(2, 2)
        self.orm.box(d)
        self.orm.insert()

    def test_select(self):
        d = ((3, 4), (1, 2))
        self.orm.box(d)
        self.orm.insert()
        self.assertSequenceEqual(
            self.orm.new().get().box.value,
            self.base.value)
        self.assertTupleEqual(
            tuple(tuple(v) for v in self.orm.naked().get().box), tuple(d))

    def test_array_insert(self):
        arr = [((3, 4), (1, 2)), ((4, 5), (2, 3))]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val, self.base_array.value)

    def test_array_select(self):
        arr = [((3, 4), (1, 2)), ((4, 5), (2, 3))]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'box')
        self.assertEqual(self.base_array.type_name, 'box[]')

if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBox, verbosity=2, failfast=True)
