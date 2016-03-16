#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import netaddr
from cargo.fields import Cidr

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestCidr(configure.NetTestCase, TestField):

    @property
    def base(self):
        return self.orm.cidr

    def test___call__(self):
        base = Cidr()
        self.assertEqual(base.value, base.empty)
        base('127.0.0.1/32')
        self.assertIsInstance(base.value, netaddr.IPNetwork)

    def test_insert(self):
        self.base('127.0.0.1/32')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(str(val.value), '127.0.0.1/32')

    def test_select(self):
        self.base('127.0.0.1/32')
        self.orm.insert(self.base)
        val = self.orm.new().desc(self.orm.uid).get()
        self.assertEqual(str(getattr(val, self.base.field_name).value),
                         '127.0.0.1/32')

    def test_array_insert(self):
        arr = ['127.0.0.1/32', '127.0.0.2/32', '127.0.0.3/32']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(list(map(str, val.value)), arr)

    def test_array_select(self):
        arr = ['127.0.0.1/32', '127.0.0.2/32', '127.0.0.3/32']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(list(map(str, val.value)), list(map(str, val_b.value)))

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'cidr')
        self.assertEqual(self.base_array.type_name, 'cidr[]')


class TestEncCidr(TestCidr):

    @property
    def base(self):
        return self.orm.enc_cidr

    def test_init(self, *args, **kwargs):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestCidr, TestEncCidr, verbosity=2, failfast=True)
