#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import netaddr
from bloom.fields import Cidr

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
        self.orm.insert(self.base)

    def test_select(self):
        self.base('127.0.0.1/32')
        self.orm.insert(self.base)
        self.assertEqual(
            str(self.orm.new().desc(self.orm.uid).get().cidr.value),
            '127.0.0.1/32')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestCidr, verbosity=2, failfast=True)
