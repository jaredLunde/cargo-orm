#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.statements.Delete`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import random
import unittest
from kola import config

from vital.security import randkey

from bloom import *
from bloom.orm import QueryState
from bloom.statements import Delete


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_pool()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
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


def populate(self):
    self.orm.use('foo').delete()
    clauses = [
        new_clause('INTO', safe('foo')),
        new_clause('RETURNING', safe('textfield'))
    ]
    self.orm.state.add(*clauses)
    values = [
        field.real_value
        for field in self.fields
        if field._should_insert() or field.default is not None
    ]
    self.orm.values(*values)
    self.orm.values(*values)
    self.orm.values(*values)
    self.orm.values(*values)
    q = INSERT(self.orm, *self.fields)
    q.execute().fetchall()

    clauses = [
        new_clause('INTO', safe('foo_b')),
        new_clause('RETURNING', safe('textfield'))
    ]
    self.orm.state.add(*clauses)

    fields = [
        new_field('text', 'bar', name='textfield', table='foo_b'),
        new_field('int', 1234, name='uid', table='foo_b')]
    values = [
        field.real_value
        for field in fields
        if field._should_insert() or field.default is not None
    ]
    self.orm.values(*values)
    self.orm.values(*values)
    self.orm.values(*values)
    self.orm.values(*values)
    q = INSERT(self.orm, *fields)
    q.execute().fetchall()
    self.orm.reset()


class TestDelete(unittest.TestCase):
    orm = ORM()
    fields = [
        new_field('text', 'bar', name='textfield', table='foo'),
        new_field('int', 1234, name='uid', table='foo')]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orm.set_table('foo')
        populate(self)

    def test___init__(self):
        q = Delete(self.orm)
        self.assertIs(q.orm, self.orm)
        self.orm.reset()

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
        self.orm.reset()

    def test__execute(self):
        populate(self)
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

    def test_pickle(self):
        clauses = [
            new_clause('FROM', safe("foo_b")),
            new_clause('WHERE', safe('foo_b.uid > 0')),
            new_clause('RETURNING', safe('*'))
        ]
        self.orm.state.add(*clauses)
        q = Delete(self.orm)
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
