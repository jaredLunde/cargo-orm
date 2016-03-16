#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import netaddr
from cargo.fields import MacAddress

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestMacAddress(configure.NetTestCase, TestField):

    @property
    def base(self):
        return self.orm.mac

    def test___call__(self):
        base = MacAddress()
        self.assertEqual(base.value, base.empty)
        base('08-00-2b-01-02-03')
        self.assertIsInstance(base.value, netaddr.EUI)

    def test_insert(self):
        self.base('08-00-2b-01-02-03')
        val = self.orm.new().insert(self.base)
        val = getattr(val, self.base.field_name)
        self.assertEqual(str(val.value), '08-00-2B-01-02-03')

    def test_select(self):
        self.base('08-00-2b-01-02-03')
        self.orm.insert(self.base)
        val = getattr(self.orm.new().desc(self.orm.uid).get(),
                      self.base.field_name)
        self.assertEqual(str(val.value), '08-00-2B-01-02-03')

    def test_array_insert(self):
        arr = ['08-00-2b-01-02-03', '08-00-2b-01-02-04']
        self.base_array(arr)
        val = self.orm.new().insert(self.base_array)
        val = getattr(val, self.base_array.field_name)
        self.assertListEqual(list(map(str, val.value)),
                             ['08-00-2B-01-02-03', '08-00-2B-01-02-04'])

    def test_array_select(self):
        arr = ['08-00-2b-01-02-03', '08-00-2b-01-02-04']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(list(map(str, val.value)),
                             list(map(str, val_b.value)))

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'macaddr')
        self.assertEqual(self.base_array.type_name, 'macaddr[]')


class TestEncMacAddress(TestMacAddress):

    @property
    def base(self):
        return self.orm.enc_mac

    def test_init(self, *args, **kwargs):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestMacAddress,
                        TestEncMacAddress,
                        verbosity=2,
                        failfast=True)
