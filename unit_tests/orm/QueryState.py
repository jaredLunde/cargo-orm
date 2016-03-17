#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.orm.QueryState`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from collections import OrderedDict
from cargo.orm import QueryState, ORM

from unit_tests import configure
from unit_tests.configure import new_clause, new_field


class TestQueryState(unittest.TestCase):

    def setUp(self):
        self.base = QueryState()

    def test_add(self):
        cls = new_clause('INTO')
        self.base.add(cls)
        self.assertIs(cls, self.base.clauses['INTO'])

    def test_parameter_inheritance(self):
        cls = new_clause('VALUES', 1, 2, 3, 4)
        self.base.add(cls)
        clsb = new_clause('VALUES', 5, 6, 7, 8)
        self.base.add(clsb)
        params = cls.params
        params.update(clsb.params)
        self.assertDictEqual(self.base.params, params)

    def test_where(self):
        field = new_field('int', name='bar', table='foo')
        cls = new_clause('WHERE', field.eq(1), join_with=' AND ')
        self.base.add(cls)
        wh = self.base.get('WHERE')
        self.assertEqual(wh.string % wh.params,
                         'WHERE foo.bar = 1')

        cls = new_clause('WHERE', field.le(1))
        self.base.add(cls)
        self.assertEqual(wh.string % wh.params,
                         'WHERE foo.bar = 1 AND foo.bar <= 1')

    def test_multi_clauses(self):
        for type in ('JOIN', 'VALUES'):
            cls = new_clause(type, 1, 2, 3, 4)
            self.base.add(cls)
            self.assertEqual(len(self.base.clauses[type]), 1)
            self.base.add(cls)
            self.assertEqual(len(self.base.clauses[type]), 2)

    def test_replace(self):
        cls = new_clause('INTO')
        self.base.add(cls)
        self.assertIs(cls, self.base.clauses['INTO'])
        clsb = new_clause('INTO')
        self.base.replace(clsb)
        self.assertIs(clsb, self.base.clauses['INTO'])

    def test_pop(self):
        cls = new_clause('INTO')
        self.base.add(cls)
        self.assertIs(cls, self.base.pop('INTO'))
        self.assertFalse(self.base.has('INTO'))

    def test_has(self):
        cls = new_clause('FROM')
        self.base.add(cls)
        self.assertTrue(self.base.has('FROM'))

    def test_get(self):
        cls = new_clause('FROM')
        self.base.add(cls)
        self.assertIs(cls, self.base.get('FROM'))
        self.base.reset()
        self.assertIsNone(self.base.get('FROM', None))

    def test_add_field(self):
        field = new_field()
        self.base.add_fields(field)
        self.assertIn(field, self.base.fields)

    def test_reset(self):
        self.base.clauses = OrderedDict([('FROM', new_clause())])
        self.base.params = {'foo': 'bar'}
        #: Subqueries
        self.base.alias = 'foo'
        self.base.one = True
        self.base.is_subquery = True
        self.base.fields = ['one']
        self.base.reset()
        self.assertEqual(len(self.base.clauses), 0)
        self.assertEqual(self.base.params, {})
        self.assertIsNone(self.base.alias)
        self.assertFalse(self.base.one)
        self.assertFalse(self.base.is_subquery)
        self.assertEqual(len(self.base.fields), 0)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestQueryState, failfast=True, verbosity=2)
