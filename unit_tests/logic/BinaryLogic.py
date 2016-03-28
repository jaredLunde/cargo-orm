#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.logic.BinaryLogic`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.logic import BinaryLogic

import psycopg2.extensions

from unit_tests import configure
from unit_tests.configure import new_field


class TestBinaryLogic(configure.LogicTestCase):

    def setUp(self):
        self.base = new_field('binary')
        self.base.field_name = 'bar'
        self.base.table = 'foo'

    def test_concat(self):
        expr = self.base.concat('foobar')
        self.validate_expression(expr,
                                 self.base,
                                 self.base.CONCAT_OP,
                                 'foobar')


    def test_octet_length(self):
        self.validate_function(
            self.base.octet_length(),
            'octet_length',
            [self.base]
        )
        self.validate_function(
            self.base.octet_length(alias='foo'),
            'octet_length',
            [self.base],
            alias='foo'
        )


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestBinaryLogic, failfast=True, verbosity=2)
