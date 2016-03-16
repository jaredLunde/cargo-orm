#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import SmallInt

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestSmallInt(configure.IntTestCase, TestField):
    '''
    value: value to populate the field with
    not_null: bool() True if the field cannot be Null
    primary: bool() True if this field is the primary key in your table
    unique: bool() True if this field is a unique index in your table
    index: bool() True if this field is a plain index in your table, that is,
        not unique or primary
    default: default value to set the field to
    validation: callable() custom validation plugin, must return True if the
        field validates, and False if it does not
    minval: int() minimum interger value
    maxval: int() maximum integer value
    '''
    @property
    def base(self):
        return self.orm.smallint

    def test_init_(self):
        self.assertEqual(self.base.value, self.base.empty)
        self.assertIsNone(self.base.primary)
        self.assertIsNone(self.base.unique)
        self.assertIsNone(self.base.index)
        self.assertIsNone(self.base.default)
        self.assertIsNone(self.base.not_null)
        self.assertEqual(self.base.minval, -32768)
        self.assertEqual(self.base.maxval, 32767)

    def test_additional_kwargs(self):
        base = SmallInt(minval=3)
        self.assertEqual(base.minval, 3)
        base = SmallInt(maxval=4)
        self.assertEqual(base.maxval, 4)
        base = SmallInt()

    def test_validate(self):
        base = SmallInt(minval=4, maxval=10)
        base(4)
        self.assertTrue(base.validate())
        base(3)
        self.assertFalse(base.validate())
        base(10)
        self.assertTrue(base.validate())
        base(11)
        self.assertFalse(base.validate())

    def test___call__(self):
        for error in ['abc', [], tuple(), set(), dict(), '4.2']:
            with self.assertRaises((ValueError, TypeError)):
                self.base(error)
        for num in [4.0, 4, 4.1, 4.9, '4']:
            self.base(num)
            self.assertEqual(self.base.value, 4)

    def test_insert(self):
        self.base(10)
        val = getattr(self.orm.new().insert(), self.base.field_name)
        self.assertEqual(val.value, 10)

    def test_select(self):
        self.base(10)
        self.orm.insert()
        self.assertEqual(
            getattr(self.orm.new().get(), self.base.field_name).value,
            self.base.value)

    def test_array_insert(self):
        arr = [1, 2, 3, 4, 5]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, arr)

    def test_array_select(self):
        arr = [1, 2, 3, 4, 5]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'smallint')
        self.assertEqual(self.base_array.type_name, 'smallint[]')


class TestEncSmallInt(TestSmallInt):

    @property
    def base(self):
        return self.orm.enc_smallint

    def test_init(self):
        pass

    def test_array_insert(self):
        arr = [1, 2, 3, 4, 5]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, arr)

    def test_array_select(self):
        arr = [1, 2, 3, 4, 5]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestSmallInt,
                        TestEncSmallInt,
                        verbosity=2,
                        failfast=True)
