#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from collections import namedtuple

from cargo import Function
from cargo.builder import *
from cargo.fields import Path, UID
from cargo.fields.geometry import PathRecord

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestPath(configure.GeoTestCase, TestField):

    @property
    def base(self):
        return self.orm.path

    def test_init(self):
        self.orm.path.clear()
        self.assertEqual(self.orm.path.value, self.orm.path.empty)
        self.assertIsNone(self.orm.path.primary)
        self.assertIsNone(self.orm.path.unique)
        self.assertIsNone(self.orm.path.index)
        self.assertIsNone(self.orm.path.default)
        self.assertIsNone(self.orm.path.not_null)

    def test___call__(self):
        d = tuple(RandData(int).tuple(2) for _ in range(3))
        self.orm.path(d)
        self.assertIsInstance(self.orm.path.value, PathRecord)
        self.assertIsInstance(self.orm.path._0, tuple)
        self.assertIsInstance(self.orm.path._1, tuple)
        self.assertIsInstance(self.orm.path._2, tuple)
        self.assertTrue(self.orm.path.closed)
        d = list(RandData(int).tuple(2) for _ in range(3))
        self.orm.path(d)
        self.assertFalse(self.orm.path.closed)
        with self.assertRaises(TypeError):
            self.orm.path(1234)
        self.orm.clear()

    def test_insert_closed(self):
        d = tuple(RandData(int).tuple(2) for _ in range(3))
        self.orm.path(d)
        self.orm.insert()

    def test_insert_open(self):
        d = list(RandData(int).tuple(2) for _ in range(3))
        self.orm.path(d)
        self.orm.insert()

    def test_select_closed(self):
        d = tuple(RandData(int).tuple(2) for _ in range(3))
        self.orm.path(d)
        self.orm.insert()
        self.assertSequenceEqual(
            self.orm.new().get().path.value,
            self.base.value)
        self.assertTupleEqual(
            tuple(self.orm.naked().get().path[:-1]), tuple(d))
        self.assertTrue(self.base.closed)

    def test_select_open(self):
        d = list(RandData(int).tuple(2) for _ in range(3))
        self.orm.path(d)
        self.orm.insert()
        self.assertSequenceEqual(
            self.orm.new().get().path.value,
            self.base.value)
        self.assertTupleEqual(
            tuple(self.orm.naked().get().path[:-1]), tuple(d))
        self.assertFalse(self.base.closed)

    def test_array_insert(self):
        arr = [self.base(list(RandData(int).tuple(2) for _ in range(3))),
               self.base(tuple(RandData(int).tuple(2) for _ in range(3)))]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val, self.base_array.value)

    def test_array_select(self):
        arr = [self.base(list(RandData(int).tuple(2) for _ in range(3))),
               self.base(tuple(RandData(int).tuple(2) for _ in range(3)))]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'path')
        self.assertEqual(self.base_array.type_name, 'path[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestPath, verbosity=2, failfast=True)
