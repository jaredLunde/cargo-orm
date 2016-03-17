#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.statements.Insert`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import random

from vital.security import randhex

from cargo import *
from cargo.orm import QueryState
from cargo.statements import Insert

from unit_tests import configure
from unit_tests.configure import new_field, new_clause, new_expression, \
                                 new_function


class TestInsert(configure.StatementTestCase):

    def test___init__(self):
        self.orm.uid(random.randint(0, 10000000))
        self.orm.payload(*self.orm.fields)
        self.orm.into('foo')
        q = Insert(self.orm)
        self.assertIs(q.orm, self.orm)

    def test_execute(self):
        self.orm.uid(random.randint(0, 10000000))
        self.orm.returning("*")
        self.orm.payload(*self.orm.fields)
        self.orm.into('foo')
        q = Insert(self.orm)
        cur = q.execute()
        item = q.orm.state.get('VALUES')
        self.assertIsNotNone(cur.fetchall()[0])

    def test__evaluate_state(self):
        self.orm.uid(random.randint(0, 10000000))
        clauses = [
            new_clause('INTO', safe('foo')),
            new_clause('RETURNING', safe('textfield'))
        ]
        self.orm.state.add(*clauses)
        self.orm.payload(*self.orm.fields)
        self.orm.into('foo')
        q = Insert(self.orm)
        self.assertIn(clauses[0].string, q.query)
        self.assertIn(clauses[1].string, q.query)
        self.assertIn(self.orm.fields[1].field_name, q.query)
        self.assertIn(self.orm.fields[0].field_name, q.query)

    def test_many(self):
        clauses = [
            new_clause('INTO', safe('foo')),
            new_clause('RETURNING', safe('textfield'))
        ]
        self.orm.state.add(*clauses)
        self.orm.values(random.randint(0, 100000), randhex(10))
        self.orm.values(random.randint(0, 100000), randhex(10))
        self.orm.payload(*(field for field in self.orm.fields
                         if field(random.randint(0, 1000000))))
        self.orm.into('foo')
        q = Insert(self.orm)
        self.assertIn(clauses[0].string, q.query)
        self.assertIn(clauses[1].string, q.query)
        self.assertIn(self.orm.fields[1].field_name, q.query)
        self.assertIn(self.orm.fields[0].field_name, q.query)
        self.assertEqual(len(q.execute().fetchall()), 3)

    '''def test_pickle(self):
        values = [
            field.value
            for field in self.orm.fields
            if field._should_insert() or field.default is not None
        ]
        self.orm.values(*values)
        self.orm.values(*values)
        q = Insert(self.orm, *self.orm.fields)
        b = pickle.loads(pickle.dumps(q))
        for k in dir(q):
            if k == '_client':
                continue
            if isinstance(
               getattr(q, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(q, k), getattr(b, k))
            else:
                self.assertTrue(
                    getattr(q, k).__class__ == getattr(b, k).__class__)'''


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestInsert)
