#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.logic.JsonLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.logic import JsonLogic

from unit_tests import configure
from unit_tests.configure import new_field


class TestJsonLogic(configure.LogicTestCase):

    def setUp(self):
        self.base = new_field('json')
        self.base.field_name = 'bar'
        self.base.table = 'foo'


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestJsonLogic, failfast=True, verbosity=2)
