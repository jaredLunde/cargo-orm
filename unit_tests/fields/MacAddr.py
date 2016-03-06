#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import netaddr
from bloom.fields import MacAddress

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
        self.orm.insert(self.base)

    def test_select(self):
        self.base('08-00-2b-01-02-03')
        self.orm.insert(self.base)
        self.assertEqual(
            str(self.orm.new().desc(self.orm.uid).get().mac.value),
            '08-00-2B-01-02-03')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestMacAddress, verbosity=2, failfast=True)
