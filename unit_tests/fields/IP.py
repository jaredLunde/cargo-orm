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
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertEqual(val, '127.0.0.1')

    def test_select(self):
        self.base('127.0.0.1')
        self.orm.insert(self.base)
        self.assertEqual(str(self.orm.new().desc(self.orm.uid).get().ip.value),
                         '127.0.0.1')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestIP, verbosity=2, failfast=True)
