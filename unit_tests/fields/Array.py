#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from kola import config

from bloom.fields import Array
from vital.debug import RandData

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Field import *


class TestArray(TestField):
    '''
    value: value to populate the field with
    not_null: bool() True if the field cannot be Null
    primary: bool() True if this field is the primary key in your table
    unique: bool() True if this field is a unique index in your table
    index: bool() True if this field is a plain index in your table, that is,
        not unique or primary
    default: default value to set the field to
    validation: callable() custom validation plugin, must return True if the field
        validates, and False if it does not
    cast: callable() to cast the values with i.e. str(), int() or float()
    dimensions: int() number of array dimensions or depth assigned to the field
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Array()
        self.assertEqual(self.base.default, [])
        self.assertEqual(self.base.cast, str)
        self.assertEqual(self.base.dimensions, 1)

    def test_additional_kwargs(self):
        arr = [1, 2, 3, 4]
        self.base = Array(value=arr)
        self.assertListEqual(self.base.value, ['1', '2', '3', '4'])
        self.base = Array(value=arr, cast=int)
        self.assertListEqual(self.base.value, [1, 2, 3, 4])
        self.base = Array(value=arr, cast=float)
        self.assertListEqual(self.base.value, [1.0, 2.0, 3.0, 4.0])

    def test_multidim_array(self):
        arr = [1, 2, 3, 4]
        self.base = Array(value=arr, cast=int, dimensions=2)
        self.assertListEqual(self.base.value, arr)
        self.base([[1, 2, 3], [1, 2, 3]])
        self.assertListEqual(self.base.value, [[1, 2, 3], [1, 2, 3]])
        self.base(None)
        with self.assertRaises(ValueError):
            self.base([[[1, 2, 3], 2, 3], [1, 2, 3]])
        self.assertEqual(self.base.value, [])
        self.base = Array(value=[[1, 2, 3], [1, 2, 3]], dimensions=2)
        self.assertListEqual(self.base.value, [['1', '2', '3'], ['1', '2', '3']])

    def test_validate(self):
        self.base = Array([1], minlen=2, maxlen=5)
        self.assertFalse(self.base.validate())
        self.base.append(2)
        self.assertTrue(self.base.validate())
        self.base.extend([3, 4, 5])
        self.assertTrue(self.base.validate())
        self.base.extend([6])
        self.assertFalse(self.base.validate())

    def test_extend(self):
        rd = RandData(str).list()
        rd2 = RandData(str).list()
        self.base = Array(rd)
        self.base.extend(rd2)
        rd.extend(rd2)
        self.assertListEqual(self.base.value, rd)

    def test_append(self):
        self.base = Array()
        self.base.append(1)
        self.base.append(2)
        self.assertListEqual(self.base.value, ['1', '2'])

    def test_insert(self):
        self.base = Array(RandData(str).list(10))
        self.base.insert(3, 'foo')
        self.assertEqual(self.base[3], 'foo')

    def test_sort(self):
        arr = [4, 1, 3, 2]
        self.base = Array(arr, cast=int)
        self.base.sort()
        arr.sort()
        self.assertListEqual(self.base.value, arr)

    def test__cast(self):
        arr = [[1], 2, [3, [[4]]]]
        self.base = Array(cast=str, dimensions=4)
        self.assertEqual(self.base._cast(arr), [['1'], '2', ['3', [['4']]]])

    def test___call__(self):
        self.assertEqual(self.base('test'), ['t', 'e', 's', 't'])
        self.assertEqual(self.base(None), [])
        self.base(['test'])
        self.assertEqual(self.base(Field.empty), ['test'])

    def test_pop(self):
        arr = [1, 2, 3]
        self.base = Array(arr)
        self.assertEqual(self.base.pop(), '1')
        self.assertEqual(self.base.pop(1), '3')

    def test_reverse(self):
        arr = [1, 2, 3]
        self.base = Array(arr, cast=int)
        a = self.base.reverse()
        b = arr.reverse()
        self.assertEqual(a, b)

    def test_remove(self):
        arr = [1, 2, 3]
        self.base = Array(arr, cast=int)
        self.assertListEqual(self.base.value, arr)

    def test___contains__(self):
        self.base = Array(RandData(str).list(10))
        self.base.append('foo')
        self.assertTrue('foo' in self.base)
        self.assertFalse('bar' in self.base)

    def test___iter__(self):
        arr = [1, 2, 3]
        self.base = Array(arr, cast=int)
        for x, y in zip(self.base, arr):
            self.assertEqual(x, y)


if __name__ == '__main__':
    # Unit test
    unittest.main()
