#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.statements.Select`
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
from bloom.statements import Select


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
    q = INSERT(self.orm, *fields)
    q.execute().fetchall()
    self.orm.reset()


class TestSelect(unittest.TestCase):
    orm = ORM()
    fields = [
        new_field('text', 'bar', name='textfield', table='foo'),
        new_field('int', 1234, name='uid', table='foo')]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orm.set_table('foo')
        #: Fill data
        populate(self)

    def test___init__(self):
        func = Functions.func('id_generator', alias="id")
        q = Select(self.orm, func)
        self.assertIs(q.orm, self.orm)
        self.assertListEqual(list(q._fields), [func])

    def test__set_fields(self):
        func = Functions.func('id_generator', alias="id")
        fields = [func, 'fish', 1234]
        fields.extend(self.fields)
        q = Select(self.orm, *fields)
        q.compile()
        compiled = q.query % q.params
        make_str = lambda x: x.name if hasattr(x, 'name') else str(x)
        self.assertIn(", ".join(map(make_str, fields)), compiled)
        for k, v in q.params.items():
            self.assertIn(k, q.query)
        self.orm.reset()

    def test_execute(self):
        func = Functions.func('id_generator', alias="id")
        fields = [
            func,
            parameterize('fish', alias='fish'),
            parameterize(1234, alias='fosh')]
        fields.extend(self.fields)
        self.orm.from_('foo')
        self.orm.where(fields[-1] == fields[-1]())
        self.orm.limit(1, 2)
        q = Select(self.orm, *fields)
        q.compile()
        compiled = q.query % q.params
        for v in fields:
            if hasattr(v, 'name'):
                v = v.name
            elif hasattr(v, 'params'):
                v = v.string % v.params
            else:
                v = str(v)
            self.assertIn(v, compiled)
        for k, v in q.params.items():
            self.assertIn(k, q.query)
        result = q.execute().fetchall()
        self.assertTrue(len(result), 2)
        self.orm.reset()

    def test__evaluate_state(self):
        '''
        if state.clause == "FROM":
            insert_clause(0, state.string)
                has_from = True
            elif 'JOIN' in state.clause:
                insert_clause(1, state.string)
            elif state.clause == "WHERE":
                insert_clause(2, state.string)
            elif state.clause == "GROUP BY":
                insert_clause(3, state.string)
            elif state.clause == "HAVING":
                insert_clause(4, state.string)
            elif state.clause == "ORDER BY":
                insert_clause(5, state.string)
            elif state.clause == "LIMIT":
                insert_clause(6, state.string)
                self.limit = int(state.args[0])
            elif state.clause == "OFFSET":
                insert_clause(7, state.string)
            elif state.clause == "FETCH":
                insert_clause(8, state.string)
            elif state.clause == "FOR":
                insert_clause(9, state.string)
        '''
        populate(self)
        clauses = [
            new_clause('FROM', safe('foo foo')),
            new_clause('WINDOW', safe('w AS (ORDER BY foo.uid DESC)')),
            new_clause('INNER JOIN', safe('foo foo2 ON foo.uid = foo2.uid')),
            new_clause('WHERE', self.fields[1] > 2),
            new_clause('GROUP BY', self.fields[1]),
            new_clause('HAVING', safe('max(foo.uid) < 1235')),
            new_clause('ORDER BY', safe('foo.uid DESC')),
            new_clause('LIMIT', 1),
            new_clause('OFFSET', 0),
            # new_clause('FETCH', safe('NEXT 1 row ONLY')),
            # new_clause('FOR', safe('UPDATE OF foo'))
        ]
        shuffled_clauses = clauses.copy()
        random.shuffle(shuffled_clauses)

        self.orm.state.add(*shuffled_clauses)
        q = Select(self.orm, self.fields[1])
        result = q.execute().fetchall()[0]
        self.assertDictEqual(result._asdict(), {'uid': 1234})
        self.orm.reset()

        clauses = [
            new_clause('FROM', safe('foo foo')),
            new_clause('INNER JOIN', safe('foo foo2 ON foo.uid = foo2.uid')),
            new_clause('WHERE', self.fields[1] > 2),
            # new_clause('HAVING', safe('max(foo.uid) < 1235')),
            new_clause('ORDER BY', safe('foo.uid DESC')),
            # new_clause('LIMIT', 1),
            # new_clause('OFFSET', 0),
            new_clause('FETCH', safe('NEXT 1 row ONLY')),
            new_clause('FOR', safe('UPDATE OF foo'))
        ]
        shuffled_clauses = clauses.copy()
        random.shuffle(shuffled_clauses)

        self.orm.state.add(*shuffled_clauses)
        q = Select(self.orm, self.fields[1])
        result = q.execute().fetchall()[0]
        self.assertDictEqual(result._asdict(), {'uid': 1234})
        self.orm.reset()

    def test_pickle(self):
        self.orm.where(safe('true') & True)
        q = Select(self.orm, self.fields[1])
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
