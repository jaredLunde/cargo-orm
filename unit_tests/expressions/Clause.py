#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.expressions.Clause`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest

from vital.tools.dicts import merge_dict
from cargo.expressions import *
from cargo import *
from cargo import fields

from unit_tests import configure
from unit_tests.configure import new_field, new_function, new_expression, \
                                 new_clause


class TestClause(unittest.TestCase):

    def test_init_(self):
        for val in ('FROM', 'from'):
            base = Clause(val, 'some_table')
            self.assertEqual(base.clause, 'FROM')
        self.assertListEqual(list(base.args), ['some_table'])

    def test__format_arg(self):
        vals = (
            (new_field(), new_field('varchar')),
            (new_field().alias('test'),),
            (new_field().alias('test', use_field_name=True),),
            (safe('some_table'),),
            ('some_table',),
            (new_expression(),),
            (new_function(),),
            (1234,)
        )
        for val in vals:
            base = Clause('FROM', *val, use_field_name=True)
            self.assertListEqual(list(base.args), list(val))
            if hasattr(val, 'params'):
                self.assertDictEqual(base.params, val.params)
                for k, v in base.params:
                    self.assertIn('%(' + k + ')s', base.string)
        for val in vals:
            base = Clause('FROM', *val, wrap=True)
            self.assertListEqual(list(base.args), list(val))
            pdicts = (v.params if hasattr(v, 'params') else {} for v in val)
            for k, v in merge_dict(*pdicts).items():
                self.assertIn(k, base.params)
            for k, v in base.params.items():
                self.assertIn('%(' + k + ')s', base.string)
            self.assertTrue(
                base.string.endswith(')') and base.string.startswith('FROM ('))


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestClause, failfast=True, verbosity=2)
