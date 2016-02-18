#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import unittest
import pickle
import copy

from vital import config
from vital.docr import Docr

from vital.sql import aliased, fields
from vital.sql.fields import Field


class Tc(object):
    pass


class TestField(unittest.TestCase):
    fields = Docr(fields)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Field(value="foo")
        self.assertEqual(self.base.value, "foo")
        self.base = Field(primary=True)
        self.assertEqual(self.base.primary, True)
        self.base = Field(unique=True)
        self.assertEqual(self.base.unique, True)
        self.base = Field(index=True)
        self.assertEqual(self.base.index, True)
        self.base = Field(default='field')
        self.assertEqual(self.base.default, 'field')
        self.base = Field(not_null=True)
        self.assertEqual(self.base.notNull, True)
        self.base = Field(validation=Tc())
        self.assertIsInstance(self.base.validation, Tc)

    def test_copy(self):
        fielda = self.base
        fieldb = self.base.copy()
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            self.assertEqual(getattr(fielda, k), getattr(fieldb, k))
        self.assertEqual(fielda.table, fieldb.table)

        fielda = self.base
        fieldb = copy.copy(self.base)
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            self.assertEqual(getattr(fielda, k), getattr(fieldb, k))
        self.assertEqual(fielda.table, fieldb.table)

    def test_deepcopy(self):
        fielda = self.base
        fieldb = copy.deepcopy(self.base)
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            if isinstance(
               getattr(fielda, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(fielda, k), getattr(fieldb, k))
            else:
                self.assertTrue(
                    getattr(fielda, k).__class__, getattr(fieldb, k).__class__)
        self.assertEqual(fielda.table, fieldb.table)

    def test_pickle(self):
        self.base.validation = None
        b = pickle.loads(pickle.dumps(self.base))
        for k in list(self.base.__slots__):
            if isinstance(
               getattr(self.base, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(self.base, k), getattr(b, k))
            else:
                self.assertTrue(
                    getattr(self.base, k).__class__ == getattr(b, k).__class__)

    def test__set_value(self):
        self.base._set_value("foo")
        self.assertEqual(self.base.data, "foo")
        self.assertEqual(self.base.value, "foo")

    def test_set_alias(self):
        field = self.base.copy()
        field.table = 'foo'
        field.field_name = 'bar'
        field.set_alias(table="foo_b")
        self.assertEqual(str(aliased(field)), "foo_b.bar")
        field.set_alias(name="foo_b")
        self.assertEqual(str(aliased(field)), "foo_b")
        field.set_alias("foo_b", "bar_b")
        self.assertEqual(str(aliased(field)), "foo_b.bar_b")

    def test___call__(self):
        self.base('foobar')
        self.assertEqual(self.base.value, 'foobar')
        self.assertEqual(self.base.data, 'foobar')
        self.assertEqual(self.base(), 'foobar')
        self.assertEqual(self.base(None), None)

    def test_validate(self):
        self.base.validation = lambda x: False
        self.assertFalse(self.base.validate())
        self.assertEqual(self.base.validation_error, "Failed validation")
        self.base.validation = lambda x: True
        self.assertTrue(self.base.validate())
        self.base.validation = lambda x: isinstance(x, self.base.__class__)
        self.assertTrue(self.base.validate())

    def test_real_value(self):
        self.assertIs(self.base.real_value,
                      self.base.value
                      if self.base.value is not self.base.empty else
                      self.base.default)


if __name__ == '__main__':
    # Unit test
    unittest.main()
