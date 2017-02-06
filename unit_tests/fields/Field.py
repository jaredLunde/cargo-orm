#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import unittest
import pickle
import copy

from cargo import aliased, Model
from cargo.fields import Field

from unit_tests import configure


class Tc(object):

    def __init__(self, field):
        self.field = field


class FieldModel(Model):
    field = Field()


class TestField(unittest.TestCase):
    orm = FieldModel()

    def setUp(self):
        self.orm.clear()

    @property
    def base(self):
        return self.orm.field

    @property
    def base_array(self):
        return getattr(self.orm, 'array_' + self.base.field_name)

    def test_init(self, *args, **kwargs):
        base = self.base.__class__()
        self.assertEqual(base.value, base.empty)
        base = self.base.__class__(primary=True)
        self.assertEqual(base.primary, True)
        base = self.base.__class__(unique=True)
        self.assertEqual(base.unique, True)
        base = self.base.__class__(index=True)
        self.assertEqual(base.index, True)
        base = self.base.__class__(default='field')
        self.assertEqual(base.default, 'field')
        base = self.base.__class__(not_null=True)
        self.assertEqual(base.not_null, True)
        base = self.base.__class__(validator=Tc)
        self.assertIsInstance(base.validator, Tc)

    def test_slots(self):
        self.assertFalse(hasattr(self.base, '__dict__'))

    def test_copy(self):
        fielda = self.base
        fieldb = self.base.copy()
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            if k == 'validator':
                self.assertEqual(fielda.validator.__class__,
                                 fieldb.validator.__class__)
            else:
                self.assertEqual(getattr(fielda, k), getattr(fieldb, k))
        self.assertEqual(fielda.table, fieldb.table)

        fielda = self.base
        fieldb = copy.copy(self.base)
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            if k == 'validator':
                self.assertEqual(fielda.validator.__class__,
                                 fieldb.validator.__class__)
            else:
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
        b = pickle.loads(pickle.dumps(self.base))
        for k in list(self.base.__slots__):
            if isinstance(
               getattr(self.base, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(self.base, k), getattr(b, k))
            else:
                self.assertTrue(
                    getattr(self.base, k).__class__ == getattr(b, k).__class__)

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
        self.assertEqual(self.base(), 'foobar')
        self.assertEqual(self.base(None), None)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestField)
