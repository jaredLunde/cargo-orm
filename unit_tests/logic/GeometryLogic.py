#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.logic.GeometryLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.logic import GeometryLogic

from unit_tests import configure
from unit_tests.configure import new_field


class TestGeometryLogic(configure.LogicTestCase):

    def setUp(self):
        self.base = new_field('point')
        self.base.field_name = 'bar'
        self.base.table = 'foo'


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestGeometryLogic, failfast=True, verbosity=2)
