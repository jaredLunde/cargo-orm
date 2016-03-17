#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.statements.Delete`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import random

from cargo import *
from cargo.statements import Delete

from unit_tests import configure
from unit_tests.configure import new_field, new_clause, new_expression, \
                                 new_function


class TestDelete(configure.StatementTestCase):

    def test___init__(self):
        q = Delete(self.orm)
        self.assertIs(q.orm, self.orm)

    def test_evaluate_state(self):
        clauses = [
            new_clause('FROM', safe("foo")),
            new_clause('USING', safe('foo_b b')),
            new_clause('WHERE', safe('foo.uid <> b.uid AND b.uid = 1234')),
            new_clause('RETURNING', safe('*'))
        ]
        shuffled_clauses = clauses.copy()
        random.shuffle(shuffled_clauses)
        self.orm.state.add(*shuffled_clauses)
        q = Delete(self.orm)
        clause_str = " ".join(q.evaluate_state())
        for clause in clauses:
            self.assertIn(clause.clause, clause_str)

    def test__execute(self):
        clauses = [
            new_clause('FROM', safe("foo")),
            new_clause('USING', safe('foo_b b')),
            new_clause('WHERE', safe('foo.uid <> b.uid AND b.uid = 1234')),
            new_clause('RETURNING', safe('foo.uid'))
        ]
        self.orm.state.add(*clauses)
        q = Delete(self.orm)
        clause_str = " ".join(q.evaluate_state())
        for clause in clauses:
            self.assertIn(clause.clause, clause_str)
        self.assertIsInstance(q.execute().fetchall(), list)
        self.orm.reset()

        clauses = [
            new_clause('FROM', safe("foo")),
            new_clause('WHERE', safe('foo.uid > 0')),
            new_clause('RETURNING', safe('*'))
        ]
        self.orm.state.add(*clauses)
        q = Delete(self.orm)
        clause_str = " ".join(q.evaluate_state())
        for clause in clauses:
            self.assertIn(clause.clause, clause_str)
        self.assertTrue(len(q.execute().fetchall()) > 0)
        self.orm.reset()

        clauses = [
            new_clause('FROM', safe("foo_b")),
            new_clause('WHERE', safe('foo_b.uid > 0')),
            new_clause('RETURNING', safe('*'))
        ]
        self.orm.state.add(*clauses)
        q = Delete(self.orm)
        clause_str = " ".join(q.evaluate_state())
        for clause in clauses:
            self.assertIn(clause.clause, clause_str)
        self.assertTrue(len(q.execute().fetchall()) > 0)
        self.orm.reset()

    '''def test_pickle(self):
        clauses = [
            new_clause('FROM', safe("foo_b")),
            new_clause('WHERE', safe('foo_b.uid > 0')),
            new_clause('RETURNING', safe('*'))
        ]
        self.orm.state.add(*clauses)
        q = Delete(self.orm)
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
    configure.run_tests(TestDelete, failfast=True, verbosity=2)
