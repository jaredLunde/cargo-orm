#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from collections import namedtuple

from bloom import Function
from bloom.builder import *
from bloom.fields import Box, UID
from bloom.fields.geometry import BoxRecord

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


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBox, verbosity=2, failfast=True)
