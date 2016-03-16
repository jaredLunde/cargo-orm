#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import psycopg2.extras
import psycopg2.extensions

from cargo import fields
from cargo.fields import *
from vital.debug import RandData

from unit_tests.fields.Field import *
from unit_tests import configure


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

    def test_init(self):
        base = Array()
        self.assertIsNone(base.default)
        self.assertIsInstance(base.type, Text)
        self.assertEqual(base.dimensions, 1)

    def test_additional_kwargs(self):
        arr = [1, 2, 3, 4]
        base = Array(value=arr, type=Text())
        self.assertListEqual(base.value, ['1', '2', '3', '4'])
        base = Array(value=arr, type=Int())
        self.assertListEqual(base.value, [1, 2, 3, 4])
        base = Array(value=arr, type=Float())
        self.assertListEqual(base.value, [1.0, 2.0, 3.0, 4.0])
        base = Array(value=arr, type=Float())
        self.assertListEqual(base.value, [1.0, 2.0, 3.0, 4.0])
        self.assertIsInstance(base.type, Float)

    def test_type(self):
        arr = Array(type=Binary())
        arr([1, 2, 3, 4])
        self.assertListEqual(arr.value, [b'1', b'2', b'3', b'4'])
        self.assertIsInstance(arr.value[0], bytes)

        arr = Array(type=Json())
        arr([1, 2, 3, 4])
        self.assertListEqual(arr.value, [1, 2, 3, 4])
        self.assertIsInstance(arr.value, list)

        arr = Array(type=Int())
        arr(['1', '2', '3', '4'])
        self.assertListEqual(arr.value, [1, 2, 3, 4])
        self.assertIsInstance(arr.value[0], int)

    def test_multidim_array(self):
        arr = [1, 2, 3, 4]
        base = Array(value=arr, type=Int(), dimensions=2)
        self.assertListEqual(base.value, arr)
        base([[1, 2, 3], [1, 2, 3]])
        self.assertListEqual(base.value, [[1, 2, 3], [1, 2, 3]])
        base(None)
        with self.assertRaises(ValueError):
            base([[[1, 2, 3], 2, 3], [1, 2, 3]])
        self.assertEqual(base.value, None)
        base = Array(value=[[1, 2, 3], [1, 2, 3]], dimensions=2, type=Text())
        self.assertListEqual(base.value,
                             [['1', '2', '3'], ['1', '2', '3']])

    def test_validate(self):
        base = Array(value=[1], minlen=2, maxlen=5)
        self.assertFalse(base.validate())
        base.append(2)
        self.assertTrue(base.validate())
        base.extend([3, 4, 5])
        self.assertTrue(base.validate())
        base.extend([6])
        self.assertFalse(base.validate())

    def test_extend(self):
        rd = RandData(str).list(100)
        rd2 = RandData(str).list(100)
        base = Array(value=rd)
        base.extend(rd2)
        rd.extend(rd2)
        self.assertListEqual(base.value, rd)

    def test_append(self):
        base = Array(type=Text())
        base.append(1)
        base.append(2)
        self.assertListEqual(base.value, ['1', '2'])

    def test_insert(self):
        base = Array(value=RandData(str).list(10))
        base.insert(3, 'foo')
        self.assertEqual(base[3], 'foo')

    def test_sort(self):
        arr = [4, 1, 3, 2]
        base = Array(value=arr, type=Int())
        base.sort()
        arr.sort()
        self.assertListEqual(base.value, arr)

    def test__cast(self):
        arr = [[1], 2, [3, [[4]]]]
        base = Array(type=Text(), dimensions=4)
        self.assertEqual(base._cast(arr), [['1'], '2', ['3', [['4']]]])

    def test___call__(self):
        base = Array()
        self.assertEqual(base('test'), ['t', 'e', 's', 't'])
        self.assertEqual(base(None), None)
        base(['test'])
        self.assertEqual(base(Field.empty), ['test'])

    def test_pop(self):
        arr = [1, 2, 3]
        base = Array(value=arr, type=Text())
        self.assertEqual(base.pop(), '1')
        self.assertEqual(base.pop(1), '3')

    def test_reverse(self):
        arr = [1, 2, 3]
        base = Array(value=arr, type=Int())
        a = base.reverse()
        b = arr.reverse()
        self.assertEqual(a, b)

    def test_remove(self):
        arr = [1, 2, 3]
        base = Array(value=arr, type=Int())
        self.assertListEqual(base.value, arr)

    def test___contains__(self):
        base = Array(value=RandData(str).list(10))
        base.append('foo')
        self.assertTrue('foo' in base)
        self.assertFalse('bar' in base)

    def test___iter__(self):
        arr = [1, 2, 3]
        base = Array(type=Int(), value=arr)
        for x, y in zip(base, arr):
            self.assertEqual(x, y)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestArray)
