#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.statements.Raw`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle

from cargo import *
from cargo.expressions import _empty
from cargo.statements import Raw

from unit_tests import configure
from unit_tests.configure import new_clause, new_field, new_expression


class TestRaw(configure.StatementTestCase):

    def test___init__(self):
        self.orm.values(1)
        q = Raw(self.orm)
        self.assertIs(q.orm, self.orm)

    def test__set_clauses(self):
        clauses = [
            new_clause(), _empty, _empty, _empty,
            new_clause('where'), new_clause('limit'),
            _empty]
        q = Raw(self.orm)
        clauses_ = list(filter(lambda x: x is not _empty, clauses))
        self.assertListEqual(list(q._filter_empty(clauses)), clauses_)

    def test_execute(self):
        clause = new_clause('SELECT', safe('1 as foo'))
        self.orm.state.add(clause)
        q = Raw(self.orm)
        cur = q.execute()
        data = cur.fetchall()
        self.assertIsInstance(data, list)
        data = data[0]
        self.assertEqual(data.foo, 1)

    def test_evaluate_state(self):
        clauses = [
            new_clause('select', '*'), new_clause(), new_clause('where', 1),
            new_clause('limit', 1)]
        self.orm.state.add(*clauses)
        q = Raw(self.orm)
        q.evaluate_state()
        cl = []
        for clause in self.orm.state:
            if isinstance(clause, list):
                cl.extend((s.string for s in clause))
            else:
                cl.append(clause.string)
        self.assertListEqual(list(q.evaluate_state()), cl)

    def test_compile(self):
        clauses = [
            new_clause('select', '*'),
            new_clause('from', 'foo'),
            new_clause('where', 'true'),
            new_clause('limit', 1)]
        self.orm.state.add(*clauses)
        q = Raw(self.orm)
        q.evaluate_state()
        self.assertEqual(
            q.query % q.params, "SELECT * FROM foo WHERE true LIMIT 1")

    '''def test_pickle(self):
        clauses = [
            new_clause('select', '*'),
            new_clause('from', 'foo'),
            new_clause('where', 'true'),
            new_clause('limit', 1)]
        self.orm.state.add(*clauses)
        q = Raw(self.orm)
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
    configure.run_tests(TestRaw, failfast=True, verbosity=2)
