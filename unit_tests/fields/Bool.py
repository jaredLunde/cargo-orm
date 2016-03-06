#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Bool

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestBool(configure.BooleanTestCase, TestField):

    @property
    def base(self):
        return self.orm.boolean

    def test_validate(self):
        a = Bool()
        self.assertTrue(a.validate())
        a = Bool(not_null=True)
        self.assertFalse(a.validate())
        a(True)
        self.assertTrue(a.validate())
        a(False)
        self.assertTrue(a.validate())
        a(None)
        self.assertFalse(a.validate())

    def test___call__(self):
        a = Bool()
        self.assertIs(a(), a.empty)
        a(True)
        self.assertTrue(a())
        a(False)
        self.assertFalse(a())
        a(1)
        self.assertTrue(a())
        a(0)
        self.assertFalse(a())
        a('0')
        self.assertTrue(a())

    def test_insert(self):
        self.base(True)
        self.orm.insert(self.base)
        self.base(False)
        self.orm.insert(self.base)
        self.base(None)
        self.orm.insert(self.base)

    def test_select(self):
        self.base(True)
        self.orm.insert(self.base)
        self.assertEqual(self.orm.new().desc(self.orm.uid).get().boolean.value,
                         True)
        self.base(False)
        self.orm.naked().insert(self.base)
        self.assertEqual(self.orm.new().desc(self.orm.uid).get().boolean.value,
                         False)
        self.base(None)
        self.orm.insert(self.base)
        self.assertEqual(self.orm.new().desc(self.orm.uid).get().boolean.value,
                         None)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBool, failfast=True, verbosity=2)
