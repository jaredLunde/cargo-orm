#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from collections import namedtuple

from bloom import Function
from bloom.builder import *
from bloom.fields import LSeg, UID
from bloom.fields.geometry import LSegRecord

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestLSeg(configure.GeoTestCase, TestField):

    @property
    def base(self):
        return self.orm.lseg

    def test_init(self):
        self.orm.lseg.clear()
        self.assertEqual(self.orm.lseg.value, self.orm.lseg.empty)
        self.assertIsNone(self.orm.lseg.primary)
        self.assertIsNone(self.orm.lseg.unique)
        self.assertIsNone(self.orm.lseg.index)
        self.assertIsNone(self.orm.lseg.default)
        self.assertIsNone(self.orm.lseg.not_null)

    def test___call__(self):
        d = RandData(int).tuple(2, 2)
        self.orm.lseg(d)
        self.assertIsInstance(self.orm.lseg.value, LSegRecord)
        self.assertIsInstance(self.orm.lseg.a, tuple)
        self.assertIsInstance(self.orm.lseg.a.x, int)
        with self.assertRaises(TypeError):
            self.orm.lseg(1234)
        self.orm.clear()

    def test_insert(self):
        d = RandData(int).tuple(2, 2)
        self.orm.lseg(d)
        self.orm.insert()
        self.orm.clear()

    def test_select(self):
        d = ((3, 4), (1, 2))
        self.orm.lseg(d)
        self.orm.insert()
        self.assertSequenceEqual(
            self.orm.new().get().lseg.value,
            self.base.value)
        self.assertTupleEqual(
            tuple(tuple(v) for v in self.orm.naked().get().lseg), tuple(d))
        self.orm.clear()


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestLSeg, verbosity=2, failfast=True)
