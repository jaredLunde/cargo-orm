#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.statements.Raw`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import unittest
from kola import config

from vital.security import randkey

from bloom import *
from bloom.expressions import _empty
from bloom.orm import QueryState
from bloom.statements import Raw


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_pool()


def new_field(type='char'):
    field = getattr(fields, type.title())()
    keyspace = 'aeioubcdlhzpwnmp'
    name = randkey(24, keyspace)
    table = randkey(24, keyspace)
    field.field_name = name
    field.table = table
    return field


def new_expression(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Expression(new_field(), '=', cast(12345))


def new_function(cast=int, alias=None):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Function('some_func', cast(12345), alias=alias)


def new_clause(name='FROM', *vals):
    vals = vals or ['foobar']
    return Clause(name, *vals)


class TestRaw(unittest.TestCase):
    orm = ORM()

    def test___init__(self):
        self.orm.values(1)
        q = Raw(self.orm)
        self.assertIs(q.orm, self.orm)
        self.orm.reset()

    def test__set_clauses(self):
        clauses = [
            new_clause(), _empty, _empty, _empty,
            new_clause('where'), new_clause('limit'),
            _empty]
        q = Raw(self.orm)
        q._set_clauses(clauses)
        self.assertNotIn(0, q.ordered_clauses)
        clauses = list(filter(lambda x: x is not _empty, clauses))
        self.assertListEqual(q.ordered_clauses, clauses)
        self.orm.reset()

    def test_execute(self):
        clause = new_clause('SELECT', aliased('1 as foo'))
        self.orm.state.add(clause)
        q = Raw(self.orm)
        cur = q.execute()
        data = cur.fetchall()
        self.assertIsInstance(data, list)
        data = data[0]
        self.assertEqual(data.foo, 1)
        self.orm.reset()

    def test__evaluate_state(self):
        clauses = [
            new_clause('select', '*'), new_clause(), new_clause('where', 1),
            new_clause('limit', 1)]
        self.orm.state.add(*clauses)
        q = Raw(self.orm)
        q._evaluate_state()
        cl = []
        for clause in self.orm.state:
            if isinstance(clause, list):
                cl.extend((s.string for s in clause))
            else:
                cl.append(clause.string)
        self.assertListEqual(q.ordered_clauses, cl)
        self.orm.reset()

    def test_compile(self):
        clauses = [
            new_clause('select', '*'),
            new_clause('from', 'foo'),
            new_clause('where', 'true'),
            new_clause('limit', 1)]
        self.orm.state.add(*clauses)
        q = Raw(self.orm)
        q._evaluate_state()
        self.assertEqual(
            q.query % q.params, "SELECT * FROM foo WHERE true LIMIT 1")
        self.orm.reset()

    def test_pickle(self):
        clauses = [
            new_clause('select', '*'),
            new_clause('from', 'foo'),
            new_clause('where', 'true'),
            new_clause('limit', 1)]
        self.orm.state.add(*clauses)
        q = Raw(self.orm)
        b = pickle.loads(pickle.dumps(q))
        for k in q.__dict__:
            if k == '_client':
                continue
            if isinstance(
               getattr(q, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(q, k), getattr(b, k))
            else:
                self.assertTrue(
                    getattr(q, k).__class__ == getattr(b, k).__class__)



if __name__ == '__main__':
    # Unit test
    unittest.main()
