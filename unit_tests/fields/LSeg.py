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
        self.assertEqual(self.base.type_name, 'lseg')
        self.assertEqual(self.base_array.type_name, 'lseg[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestLSeg, verbosity=2, failfast=True)
