#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.statements.Update`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import random
import unittest
from kola import config

from vital.security import randkey

from cargo import *
from cargo import fields
from cargo.orm import QueryState
from cargo.statements import Update


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_pool()


def new_field(type='varchar', value=None, name=None, table=None):
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
    self.orm.reset()
    self.orm.use('foo').delete()
    clauses = [
        new_clause('INTO', safe('foo')),
        new_clause('RETURNING', safe('textfield'))
    ]
    self.orm.state.add(*clauses)
    values = [
        field
        for field in self.fields
        if field._should_insert() or field.default is not None
    ]
    self.orm.values(*values)
    self.orm.values(*values)
    self.orm.values(*values)
    q = Insert(self.orm)
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
        field
        for field in fields
        if field._should_insert() or field.default is not None
    ]
    self.orm.values(*values)
    self.orm.values(*values)
    self.orm.values(*values)
    q = Insert(self.orm)
    q.execute().fetchall()
    self.orm.reset()


class TestUpdate(unittest.TestCase):
    orm = ORM()
    fields = [
        new_field('text', 'bar', name='textfield', table='foo'),
        new_field('int', 1234, name='uid', table='foo')]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orm.set_table('foo')
        self.orm.reset()

    def test___init__(self):
        self.fields.append(self.fields[0] == 'uid')
        self.orm.set(*self.fields)
        q = Update(self.orm)
        self.assertIs(q.orm, self.orm)
        self.fields.pop(-1)
        self.orm.reset()

    def test_execute(self):
        populate(self)
        self.fields[0].value = 'bar_2'
        self.orm.set(*[
            field == field.value for field in self.fields])
        self.orm.returning(*self.fields)
        q = Update(self.orm)
        result = q.execute().fetchall()
        self.assertTrue(len(result) > 0)
        self.orm.reset()

        self.fields[0].value = 'bar'
        self.orm.set(*[
            field == field.value for field in self.fields])
        self.orm.returning(*self.fields[:1])
        q = Update(self.orm)
        result = q.execute().fetchall()
        self.assertTrue(len(result) > 0)
        self.orm.reset()

    def test_evaluate_state(self):
        '''
        if state.clause == "SET":
            query_clauses[0] = state.string
        elif state.clause == "FROM":
            query_clauses[1] = state.string
        elif state.clause == "WHERE":
            query_clauses[2] = state.string
        elif state.clause == "RETURNING":
            query_clauses[3] = state.string
        '''
        populate(self)
        clauses = [
            new_clause('SET', safe("textfield = b.textfield")),
            new_clause('FROM', safe('foo_b b')),
            new_clause('WHERE', safe('foo.uid = b.uid')),
            new_clause('RETURNING', safe('*'))
        ]
        shuffled_clauses = clauses.copy()
        random.shuffle(shuffled_clauses)
        self.orm.state.add(*shuffled_clauses)
        q = Update(self.orm)
        self.assertTrue(len(q.execute().fetchall()) > 1)

        clause_str = " ".join(q.evaluate_state())
        for clause in clauses:
            self.assertIn(clause.clause, clause_str)
        self.orm.reset()

        field_b = new_field('int', 1234, name='uid', table='foo_b')
        field_b.set_alias(table='b')
        self.orm.where(self.fields[1] == aliased(field_b))
        self.orm.set(
            self.fields[1] == aliased(field_b),
            self.fields[0] == self.fields[0].value)
        self.orm.use(field_b.table, alias='b')
        self.orm.returning(aliased(field_b))
        q = Update(self.orm)
        result = q.execute().fetchall()
        self.assertTrue(len(result) > 1)
        self.assertIn('uid', result[0]._fields)
        self.orm.reset()

    '''def test_pickle(self):
        field_b = new_field('int', value=1234, name='uid', table='foo_b')
        field_b.set_alias(table='b')
        self.orm.where(self.fields[1] == aliased(field_b))
        self.orm.set(
            self.fields[1] == aliased(field_b),
            self.fields[0] == self.fields[0].value)
        self.orm.use(field_b.table, alias='b')
        self.orm.returning(aliased(field_b))
        q = Update(self.orm)
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
    unittest.main()
