#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import netaddr
from bloom.fields import IP

from unit_tests.fields.Field import TestField
from unit_tests import configure


class Request(object):
    @property
    def remote_addr(self):
        return '68.161.102.23'

request = Request()


class TestIP(configure.NetTestCase, TestField):

    @property
    def base(self):
        return self.orm.ip

    def test___call__(self):
        base = IP(request=request)
        self.assertEqual(base.value, base.empty)
        base(request.remote_addr)
        self.assertIsInstance(base.value, netaddr.IPAddress)
        base(IP.current)
        self.assertEqual(str(base.value), request.remote_addr)

    def test_default(self):
        base = IP(request=request, default=IP.current)
        self.assertEqual(str(base.default), request.remote_addr)

    def test_insert(self):
        self.base('127.0.0.1')
        self.orm.insert(self.base)
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(str(val.value), '127.0.0.1')

    def test_select(self):
        self.base('127.0.0.1')
        self.orm.insert(self.base)
        val = self.orm.new().desc(self.orm.uid).get()
        self.assertEqual(str(getattr(val, self.base.field_name).value),
                         '127.0.0.1')

    def test_array_insert(self):
        arr = ['127.0.0.1', '127.0.0.2', '127.0.0.3']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(list(map(str, val.value)), arr)

    def test_array_select(self):
        arr = ['127.0.0.1', '127.0.0.2', '127.0.0.3']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(list(map(str, val.value)),
                             list(map(str, val_b.value)))

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'inet')
        self.assertEqual(self.base_array.type_name, 'inet[]')


class TestEncIP(TestIP):

    @property
    def base(self):
        return self.orm.enc_ip

    def test_init(self, *args, **kwargs):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestIP, TestEncIP, verbosity=2, failfast=True)
