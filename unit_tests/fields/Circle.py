#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from collections import namedtuple

from bloom import Function
from bloom.builder import *
from bloom.fields import Circle, UID
from bloom.fields.geometry import CircleRecord

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestCircle(configure.GeoTestCase, TestField):

    @property
    def base(self):
        return self.orm.circle

    def test_init(self):
        self.orm.circle.clear()
        self.assertEqual(self.orm.circle.value, self.orm.circle.empty)
        self.assertIsNone(self.orm.circle.primary)
        self.assertIsNone(self.orm.circle.unique)
        self.assertIsNone(self.orm.circle.index)
        self.assertIsNone(self.orm.circle.default)
        self.assertIsNone(self.orm.circle.not_null)

    def test___call__(self):
        d = [RandData(int).tuple(2)]
        d.append(RandData().randint)
        self.orm.circle(d)
        self.assertIsInstance(self.orm.circle.value, CircleRecord)
        self.assertIsInstance(self.orm.circle.center, tuple)
        self.assertIsInstance(self.orm.circle.center.x, int)
        self.assertIsInstance(self.orm.circle.center.y, int)
        self.assertIsInstance(self.orm.circle.radius, int)
        with self.assertRaises(TypeError):
            self.orm.circle(1234)

    def test_insert(self):
        d = [RandData(int).tuple(2)]
        d.append(RandData().randint)
        self.orm.circle(d)
        self.orm.insert()

    def test_select(self):
        d = [RandData(int).tuple(2)]
        d.append(RandData().randint)
        self.orm.circle(d)
        self.orm.insert()
        self.assertSequenceEqual(
            self.orm.new().get().circle.value,
            self.base.value)
        self.assertSequenceEqual(self.orm.naked().get().circle, d)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestCircle, verbosity=2, failfast=True)
