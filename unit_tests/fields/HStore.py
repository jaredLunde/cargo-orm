#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from cargo.fields import HStore

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestHStore(configure.KeyValueTestCase, TestField):

    @property
    def base(self):
        return self.orm.hstore_field

    def test_init_(self):
        base = HStore()
        rd = RandData(str).dict(4, 2)
        base(rd)
        self.assertIsInstance(base.value, dict)
        with self.assertRaises(ValueError):
            base('foo')

    def test___call__(self):
        self.base(RandData(str).dict(4, 2))
        self.assertIsInstance(self.base.value, dict)
        self.base.clear()

    def test_insert(self):
        self.base(RandData(str).dict(10))
        self.assertIsInstance(self.orm.new().save(), self.orm.__class__)

    def test_select(self):
        self.base(RandData(str).dict(10))
        self.orm.new().save()
        orm = self.orm.new().desc(self.orm.uid)
        d = getattr(orm.get(), self.base.field_name).value
        self.assertDictEqual(d, self.base.value)
        self.assertIsNot(d, self.base.value)

    def test_array_insert(self):
        arr = [RandData(str).dict(10), RandData(str).dict(10)]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, arr)

    def test_array_select(self):
        arr = [RandData(str).dict(10), RandData(str).dict(10)]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'hstore')
        self.assertEqual(self.base_array.type_name, 'hstore[]')


class TestEncHStore(TestHStore):

    @property
    def base(self):
        return self.orm.enc_hstore

    def test_init(self):
        pass


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestHStore, TestEncHStore, failfast=True, verbosity=2)
