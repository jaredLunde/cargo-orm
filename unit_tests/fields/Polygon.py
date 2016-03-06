#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from collections import namedtuple

from bloom import Function
from bloom.builder import *
from bloom.fields import Polygon, UID
from bloom.fields.geometry import PolygonRecord

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestPolygon(configure.GeoTestCase, TestField):

    @property
    def base(self):
        return self.orm.poly

    def test_init(self):
        self.orm.poly.clear()
        self.assertEqual(self.orm.poly.value, self.orm.poly.empty)
        self.assertIsNone(self.orm.poly.primary)
        self.assertIsNone(self.orm.poly.unique)
        self.assertIsNone(self.orm.poly.index)
        self.assertIsNone(self.orm.poly.default)
        self.assertIsNone(self.orm.poly.not_null)

    def test___call__(self):
        d = tuple(RandData(int).tuple(2) for x in range(10))
        self.orm.poly(d)
        self.assertIsInstance(self.orm.poly.value, PolygonRecord)
        self.assertIsInstance(self.orm.poly._0, tuple)
        self.assertIsInstance(self.orm.poly._0.x, int)
        with self.assertRaises(TypeError):
            self.orm.poly(1234)

    def test_insert(self):
        d = tuple(RandData(int).tuple(2) for x in range(10))
        self.orm.poly(d)
        self.orm.insert()

    def test_select(self):
        d = tuple(RandData(int).tuple(2) for x in range(10))
        self.orm.poly(d)
        self.orm.insert()
        self.assertSequenceEqual(
            self.orm.new().get().poly.value,
            self.base.value)
        self.assertSequenceEqual(self.orm.naked().get().poly, d)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestPolygon, verbosity=2, failfast=True)
