#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from collections import namedtuple

from bloom import Function
from bloom.builder import *
from bloom.fields import Point, UID
from bloom.fields.geometry import PointRecord

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestPoint(configure.GeoTestCase, TestField):

    @property
    def base(self):
        return self.orm.point

    def test_init(self):
        self.orm.point.clear()
        self.assertEqual(self.orm.point.value, self.orm.point.empty)
        self.assertIsNone(self.orm.point.primary)
        self.assertIsNone(self.orm.point.unique)
        self.assertIsNone(self.orm.point.index)
        self.assertIsNone(self.orm.point.default)
        self.assertIsNone(self.orm.point.not_null)

    def test___call__(self):
        d = RandData(int).tuple(2)
        self.orm.point(d)
        self.assertIsInstance(self.orm.point.value, PointRecord)
        self.assertIsInstance(self.orm.point.x, int)
        self.assertIsInstance(self.orm.point.y, int)
        with self.assertRaises(TypeError):
            self.orm.point(1234)

    def test_insert(self):
        d = RandData(int).tuple(2)
        self.orm.point(d)
        self.orm.insert()

    def test_select(self):
        d = RandData(int).tuple(2)
        self.orm.point(d)
        self.orm.insert()
        self.assertSequenceEqual(
            self.orm.new().get().point.value,
            self.base.value)
        self.assertSequenceEqual(self.orm.naked().get().point, d)

    def test_array_insert(self):
        arr = [RandData(int).tuple(2), RandData(int).tuple(2)]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val, self.base_array.value)

    def test_array_select(self):
        arr = [RandData(int).tuple(2), RandData(int).tuple(2)]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'point')
        self.assertEqual(self.base_array.type_name, 'point[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestPoint, verbosity=2, failfast=True)
