#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from vital.debug import RandData
from bloom.fields import HStore

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
        self.orm.naked().save()
        orm = self.orm.new().desc(self.orm.uid)
        d = orm.get().hstore_field.value
        self.assertDictEqual(d, self.base.value)
        self.assertIsNot(d, self.base.value)



if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestHStore, failfast=True, verbosity=2)
