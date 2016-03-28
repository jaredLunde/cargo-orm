#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.logic.NetworkingLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.logic import NetworkingLogic

from unit_tests import configure
from unit_tests.configure import new_field


class TestNetworkingLogic(configure.LogicTestCase):

    def setUp(self):
        self.base = new_field('ip')
        self.base.field_name = 'bar'
        self.base.table = 'foo'


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestNetworkingLogic, failfast=True, verbosity=2)
