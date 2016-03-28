#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.logic.HStoreLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.logic import HStoreLogic

from unit_tests import configure
from unit_tests.configure import new_field


class TestHStoreLogic(configure.LogicTestCase):

    def setUp(self):
        self.base = new_field('hstore')
        self.base.field_name = 'bar'
        self.base.table = 'foo'


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestHStoreLogic, failfast=True, verbosity=2)
