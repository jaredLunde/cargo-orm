#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo import safe
from cargo.fields import Serial

from unit_tests.fields.Int import TestInt
from unit_tests import configure


class TestSerial(configure.IdentifierTestCase, TestInt):
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
    orm = configure.SerialModel()

    @property
    def base(self):
        return self.orm.serial

    def test_init_(self):
        base = Serial()
        self.assertEqual(base.value, base.empty)
        self.assertTrue(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsNone(base.default)
        self.assertIsNone(base.not_null)
        self.assertEqual(base.minval, 1)
        self.assertEqual(base.maxval, 2147483647)

    def test_value(self):
        self.base(1234)
        self.assertIs(self.base.value, self.base.value)
        self.base.clear()
        self.assertIs(self.base.value, self.base.empty)
        self.base(None)
        self.assertIsNone(self.base.value)

    def test_insert(self):
        self.base(1234)
        self.orm.insert(self.base)
        self.base.clear()
        val1 = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        val2 = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertTrue(val2 - val1 == 1)

    def test_select(self):
        self.orm.insert(self.base)
        self.assertTrue(
            getattr(self.orm.new().desc(self.base).get(),
                    self.base.field_name).value > 0)

    def test_array_insert(self):
        pass

    def test_array_select(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'integer')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestSerial, verbosity=2, failfast=True)
