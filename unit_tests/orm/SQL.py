#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.orm.ORM`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import copy
import types
import pickle
import random
import psycopg2.extras
from cnamedtuple import namedtuple

from cargo import *
from cargo.orm import ORM, QueryState

from unit_tests import configure
from unit_tests.configure import new_field, new_function, new_expression,\
                                 new_clause


class TestORM(configure.BaseTestCase):
    client = Postgres()

    @staticmethod
    def setUpClass():
        configure.setup()
        configure.Plan(configure.Foo()).execute()

    def setUp(self):
        self.orm = ORM(schema='cargo_tests')
        local_client.clear()

    def tearDown(self):
        self.orm.use('foo').where(True).delete()

    def populate(self):
        self.orm.use('foo').where(True).delete()
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        self.orm.values(12345, 'bar')
        self.orm.values(123456, 'bar')
        self.orm.values(1234567, 'bar')
        self.orm.returning(f1, f2)
        res = self.orm.insert(f1, f2)

    def test___init__(self):
        orm = ORM()
        self.assertIsNone(orm._client)
        self.assertFalse(orm._multi)
        self.assertIsInstance(orm._state, QueryState)
        self.assertEqual(orm._debug, False)
        orm.close()
        orm = ORM(client=self.client, debug=True)
        self.assertTrue(orm._debug)
        self.assertIs(orm._client, self.client)
        orm.close()

    def test_client(self):
        if local_client.get('db'):
            del local_client['db']
        orm = ORM(client=None)
        self.assertIsInstance(orm.db, Postgres)
        self.assertIs(orm.db, local_client.get('db'))
        self.assertIsNot(orm.db, self.client)
        orm.close()
        orm = ORM(client=self.client)
        self.assertIs(orm.db, self.client)
        orm.close()

    def test_close(self):
        # Client
        orm = ORM(client=Postgres())
        self.assertFalse(orm.db.connection.closed)
        orm.close()
        self.assertTrue(orm.db._connection.closed)

        # Pool
        orm = ORM(client=PostgresPool(1, 2))
        self.assertFalse(orm.db.pool.closed)
        orm.close()
        self.assertTrue(orm.db._pool.closed)

    def test_context_manager(self):
        # Client
        with ORM(client=Postgres()) as orm:
            self.assertFalse(orm.db._connection.closed)
        self.assertTrue(orm.db._connection.closed)

        # Pool
        with ORM(client=PostgresPool(1, 2)) as orm:
            self.assertFalse(orm.db._pool.closed)
        self.assertTrue(orm.db._pool.closed)

    def test_set_table(self):
        self.orm.set_table('foo')
        self.assertEqual(self.orm.table, 'foo')
        self.orm.set_table('bar')
        self.assertEqual(self.orm.table, 'bar')
        self.orm.set_table('foo')
        self.assertEqual(self.orm.table, 'foo')

    def test_window(self):
        field = new_field(table='foo', name='bar')
        self.orm.window('w',
                        Clause('PARTITION BY', field),
                        Clause('ORDER BY', field))
        self.assertTrue(self.orm.state.has('WINDOW'))
        self.assertEqual(self.orm.state.clauses.popitem(last=True)[1].string,
                         'WINDOW w AS (PARTITION BY foo.bar ORDER BY foo.bar)')

        self.orm.window('w', partition_by=field)
        self.assertEqual(self.orm.state.clauses.popitem(last=True)[1].string,
                         'WINDOW w AS (PARTITION BY foo.bar)')

        self.orm.window('w', order_by=field)
        self.assertEqual(self.orm.state.clauses.popitem(last=True)[1].string,
                         'WINDOW w AS (ORDER BY foo.bar)')

        self.orm.window('w', order_by=field.desc(), partition_by=field)
        self.assertEqual(self.orm.state.clauses.popitem(last=True)[1].string,
                         'WINDOW w AS (PARTITION BY foo.bar ORDER BY ' +
                         'foo.bar DESC)')

    def test_from(self):
        for f in (self.orm.use, self.orm.from_):
            # No alias
            s = f('foo')
            self.assertTrue(self.orm.state.has('FROM'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIsInstance(clause.args[0], safe)
            self.assertEqual(clause.args[0].string, 'foo')
            self.assertEqual(clause.alias, None)
            self.assertIs(s, self.orm)
            self.orm.reset()
            # Alias
            s = f('foo', alias='bar')
            self.assertTrue(self.orm.state.has('FROM'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIsInstance(clause.args[0], safe)
            self.assertEqual(clause.args[0].string, 'foo')
            self.assertEqual(clause.alias, 'bar')
            self.assertIs(s, self.orm)

    def test_join(self):
        # fields
        fielda = new_field('int', table='foo', name='bar')
        fieldb = new_field('int', table='foo_b', name='baz')
        fieldc = new_field('int', table='foo_c', name='boz')
        fieldd = new_field('int', table='foo_c', name='buz')
        # joins
        join_types = (
            self.orm.join, self.orm.left_join, self.orm.right_join,
            self.orm.full_join, self.orm.cross_join
        )
        for join in join_types:
            _args = (
                #: Singular USING field
                ([], {'using': fielda}, '{} foo USING (bar)'),
                #: Singular USING field w/ alias
                ([], {'using': (fielda), 'alias': 'foo_alias'},
                 '{} foo foo_alias USING (bar)'),
                #: Multpile USING fields
                ([], {'using': (fielda, fieldb)}, '{} foo USING (bar, baz)'),
                #: Multpile USING fields w/ alias
                ([], {'using': (fielda, fieldb), 'alias': 'foo_alias'},
                 '{} foo foo_alias USING (bar, baz)'),
                #: Single expression
                ([fieldb == fielda], {}, '{} foo_b ON foo_b.baz = foo.bar'),
                #: Single expression w/ alias
                ([fieldb == fielda], {'alias': 'foo_alias'},
                 '{} foo_b foo_alias ON foo_alias.baz = foo.bar'),
                #: Multiple expressions
                ([(fieldb == fielda) & (fieldc == fielda)], {},
                 '{} foo_b ON foo_b.baz = foo.bar AND foo_c.boz = foo.bar'),
                #: Multiple expressions w/ alias
                ([(fieldb == fielda) & (fieldc == fielda)],
                 {'alias': 'foo_alias'},
                 '{} foo_b foo_alias ON foo_alias.baz = foo.bar AND ' +
                 'foo_c.boz = foo.bar'),
                #: Multiple ON= expressions
                ([fieldb],
                 {'on': (fieldb == fielda) & (fieldc == fielda) & \
                        (fieldc == fieldd)},
                 '{} foo_b ON foo_b.baz = foo.bar AND foo_c.boz = foo.bar ' +
                 'AND foo_c.boz = foo_c.buz'),
                #: Multiple ON expressions w/ alias
                ([fieldb],
                 {'on': (fieldb == fielda) & (fieldb == fieldc) & \
                        (fieldc == fieldd),
                  'alias': 'foo_alias'
                 },
                 '{} foo_b foo_alias ON foo_alias.baz = foo.bar AND '+
                 'foo_alias.baz = foo_c.boz AND foo_c.boz = foo_c.buz'),
                #: Auto ON expressions
                ([fielda, fieldb], {}, '{} foo ON foo.bar = foo_b.baz'),
                #: Auto ON expressions w/ alias
                ([fielda, fieldb], {'alias': 'foo_alias'},
                 '{} foo foo_alias ON foo_alias.bar = foo_b.baz')
            )
            for args, kwargs, validate in _args:
                s = join(*args, **kwargs)
                clause = self.orm.state.clauses.popitem(last=True)[1][0]
                self.assertIs(s, self.orm)
                self.assertEqual(str(clause.string % clause.params),
                                 validate.format(clause.clause))

    def test_where(self):
        # Single Expression
        s = self.orm.where(safe(1) & 1)
        self.assertTrue(self.orm.state.has('WHERE'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'WHERE 1 AND 1')
        self.orm.reset()

        # Comma'd expressions
        s = self.orm.where(1, 1)
        self.assertTrue(self.orm.state.has('WHERE'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'WHERE 1 AND 1')
        self.orm.reset()

        # Multiple filters
        s = self.orm.where(1, 1)
        s = self.orm.where(2, 2)
        self.assertTrue(self.orm.state.has('WHERE'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(
            clause.string % clause.params, 'WHERE 1 AND 1 AND 2 AND 2')

    def test_into(self):
        # No alias
        s = self.orm.into('foo')
        self.assertTrue(self.orm.state.has('INTO'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIsInstance(clause.args[0], safe)
        self.assertEqual(clause.args[0].string, 'foo')
        self.assertEqual(clause.alias, None)
        self.assertIs(s, self.orm)
        self.orm.reset()
        # Alias
        s = self.orm.into('foo', alias='bar')
        self.assertTrue(self.orm.state.has('INTO'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIsInstance(clause.args[0], safe)
        self.assertEqual(clause.args[0].string, 'foo')
        self.assertEqual(clause.alias, 'bar')
        self.assertIs(s, self.orm)

    def test_values(self):
        s = self.orm.values(1, 2)
        self.assertTrue(self.orm.state.has('VALUES'))
        clause = self.orm.state.clauses.popitem(last=True)[1][0]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'VALUES (1, 2)')
        s = self.orm.values(new_field('int', 5), new_field('int', 6))
        self.assertTrue(self.orm.state.has('VALUES'))
        clause = self.orm.state.clauses.popitem(last=True)[1][0]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'VALUES (5, 6)')

    def test_set(self):
        field = new_field('int', name='bar', table='foo')
        s = self.orm.set(field == 2, 2)
        self.assertTrue(self.orm.state.has('SET'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'SET bar = 2, 2')

    def test_group_by(self):
        field = new_field('int', name='bar', table='foo')
        s = self.orm.group_by(field)
        self.assertTrue(self.orm.state.has('GROUP BY'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'GROUP BY foo.bar')
        self.orm.reset()

        # Using field name
        field = new_field('int', name='bar', table='foo')
        s = self.orm.group_by(field, use_field_name=True)
        self.assertTrue(self.orm.state.has('GROUP BY'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'GROUP BY bar')

    def test_order_by(self):
        field = new_field('int', name='bar', table='foo')
        s = self.orm.order_by(field.desc())
        self.assertTrue(self.orm.state.has('ORDER BY'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'ORDER BY foo.bar DESC')
        self.orm.reset()

        # Using field name
        field = new_field('int', name='bar', table='foo')
        s = self.orm.order_by(field.desc(), use_field_name=True)
        self.assertTrue(self.orm.state.has('ORDER BY'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'ORDER BY bar DESC')
        self.orm.reset()

        # Multiple fields
        field = new_field('int', name='bar', table='foo')
        fieldb = new_field('int', name='bar_b', table='foo')
        s = self.orm.order_by(field.desc(), fieldb.asc(), use_field_name=True)
        self.assertTrue(self.orm.state.has('ORDER BY'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(
            clause.string % clause.params, 'ORDER BY bar DESC, bar_b ASC')

    def test_asc(self):
        field = new_field('int', name='bar', table='foo')
        for v, f in (
            ('foo.bar', field), ('num_posts', safe('num_posts')),
            ('num_posts', safe('num_posts'))
        ):
            s = self.orm.asc(f)
            self.assertTrue(self.orm.state.has('ORDER BY'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            self.assertEqual(
                clause.string % clause.params, 'ORDER BY {} ASC'.format(v))
            self.orm.reset()

    def test_desc(self):
        field = new_field('int', name='bar', table='foo')
        for v, f in (
            ('foo.bar', field), ('num_posts', safe('num_posts')),
            ('num_posts', safe('num_posts'))
        ):
            s = self.orm.desc(f)
            self.assertTrue(self.orm.state.has('ORDER BY'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            self.assertEqual(
                clause.string % clause.params, 'ORDER BY {} DESC'.format(v))
            self.orm.reset()

    def test_limit(self):
        for f in (1, '1000', Subquery(Query(orm=self.orm, query='SELECT 1'))):
            s = self.orm.limit(f)
            self.assertTrue(self.orm.state.has('LIMIT'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            self.assertEqual(
                clause.string % clause.params, 'LIMIT {}'.format(str(f)))
            self.orm.reset()

    def test_limit_offset(self):
        for f in (
            (1, 2),
            ('1000', 1001),
            (Query(orm=self.orm, query='SELECT 1'), 10)
        ):
            s = self.orm.limit(*f)
            self.assertTrue(self.orm.state.has('LIMIT'))
            self.assertTrue(self.orm.state.has('OFFSET'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            self.assertEqual(
                clause.string % clause.params, 'OFFSET {}'.format(str(f[0])))
            self.orm.reset()

    def test_fetch(self):
        for f in (
            ('FIRST', 3, ''),
            ('LAST', 1001, ''),
            ('NEXT', '', ''),
            ('ABSOLUTE', 5, 'BACKWARD')
        ):
            s = self.orm.fetch(*f)
            self.assertTrue(self.orm.state.has('FETCH'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            self.assertEqual(
                clause.string % clause.params,
                'FETCH {} {} {}'.format(*f).strip())
            self.orm.reset()

    def test_offset(self):
        for f in (1, '1000', Query(orm=self.orm, query='SELECT 1')):
            s = self.orm.offset(f)
            self.assertTrue(self.orm.state.has('OFFSET'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            self.assertEqual(
                clause.string % clause.params, 'OFFSET {}'.format(str(f)))
            self.orm.reset()

    def test_one(self):
        s = self.orm.one()
        self.assertTrue(self.orm.state.one)
        self.assertIs(s, self.orm)

    def test_page(self):
        for f in (
            (1, 25),
            ('1000', 1001),
        ):
            s = self.orm.page(*f)
            self.assertTrue(self.orm.state.has('LIMIT'))
            self.assertTrue(self.orm.state.has('OFFSET'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            offset = (int(f[0]) * f[1]) - f[1]
            self.assertEqual(
                clause.string % clause.params, 'OFFSET {}'.format(offset))
            self.orm.reset()

    def test_having(self):
        field = new_field('int')
        for f in (
            (field.max() < 40),
            (Subquery(Query(orm=self.orm, query='SELECT 1')) > 0),
        ):
            s = self.orm.having(f)
            self.assertTrue(self.orm.state.has('HAVING'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            self.assertEqual(
                clause.string % clause.params,
                'HAVING {}'.format(str(f) % f.params))
            self.orm.reset()

    def test_for_update(self):
        s = self.orm.for_update()
        self.assertTrue(self.orm.state.has('FOR'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'FOR UPDATE')

    def test_for_share(self):
        s = self.orm.for_share()
        self.assertTrue(self.orm.state.has('FOR'))
        clause = self.orm.state.clauses.popitem(last=True)[1]
        self.assertIs(s, self.orm)
        self.assertEqual(clause.string % clause.params, 'FOR SHARE')

    def test_returning(self):
        fields = (
            (new_field('text'), new_field('int'), new_field('email')),
            (new_field('decimal'), None, None),
            (None, None, None),
            ('*', None, None)
        )
        for f in fields:
            s = self.orm.returning(*f)
            self.assertTrue(self.orm.state.has('RETURNING'))
            clause = self.orm.state.clauses.popitem(last=True)[1]
            self.assertIs(s, self.orm)
            vals = []
            for x in f:
                if x is None:
                    continue
                if hasattr(x, 'name'):
                    vals.append(x.name)
                    continue
                if len(x):
                    vals.append(str(x))
            self.assertEqual(
                (clause.string % clause.params),
                'RETURNING {}'.format(', '.join(vals) if vals else '*'))
            self.orm.reset()

    def test_on(self):
        for f in (
            (Clause('CONFLICT', Clause('DO UPDATE')),),
            (F.new("CONFLICT", new_field('text')), Clause('DO UPDATE'))
        ):
            s = self.orm.on(*f, join_with=" ")
            self.assertTrue(self.orm.state.has('ON'))
            self.assertIs(s, self.orm)
            clause = self.orm.state.clauses.popitem(last=True)[1]
            v = " ".join(str(_) for _ in f)
            self.assertEqual(
                (clause.string % clause.params), 'ON {}'.format(v))
            self.orm.reset()

    def test_insert(self):
        # Plain INSERT
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        self.orm.values(12345, 'bar')
        self.orm.values(123456, 'bar')
        self.orm.returning(f1, f2)
        res = self.orm.insert(f1, f2)
        self.assertEqual(len(res), 2)
        self.assertEqual(len(res[1]), 2)

        # Non-executing INSERT
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        self.orm.values(12345, 'bar')
        self.orm.values(123456, 'bar')
        self.orm.returning(f1, f2)
        res = self.orm.dry().insert(f1, f2)
        self.assertIsInstance(res, Query)
        self.assertFalse(len(self.orm.queries))
        self.assertFalse(len(self.orm.state.clauses))

        # Subquery INSERT
        self.orm.subquery()
        self.orm.values(12345, 'bar')
        self.orm.values(123456, 'bar')
        self.orm.returning(f1, f2)
        res = self.orm.insert(f1, f2)
        self.assertIsInstance(res, Subquery)
        self.orm.reset()

        # Multi INSERT
        self.orm.multi()
        self.orm.values(12345, 'bar')
        self.orm.values(123456, 'bar')
        self.orm.returning(f1, f2)
        res = self.orm.insert(f1, f2)
        self.assertIs(res, self.orm)
        self.assertEqual(len(self.orm.queries), 1)

    def test_select(self):
        self.populate()
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        # Plain SELECT
        self.orm.use(f1.table)
        self.orm.where(f2 == 'bar')
        self.orm.limit(2)
        res = self.orm.select(f1, f2)
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 2)

        # Non-executing SELECT
        self.orm.use(f1.table)
        self.orm.where(f2 == 'bar')
        self.orm.limit(2)
        res = self.orm.dry().select(f1, f2)
        self.assertIsInstance(res, Query)
        self.assertFalse(len(self.orm.queries))
        self.assertFalse(len(self.orm.state.clauses))

        # Subquery SELECT
        self.orm.subquery()
        self.orm.use(f1.table)
        self.orm.where(f2 == 'bar')
        self.orm.limit(2)
        res = self.orm.select(f1, f2)
        self.assertIsInstance(res, Subquery)

        # Multi SELECT
        self.orm.multi()
        self.orm.where(f2 == 'bar')
        self.orm.limit(2)
        res = self.orm.select(f1, f2)
        self.assertIs(res, self.orm)
        self.assertEqual(len(self.orm.queries), 1)
        self.orm.reset_multi()
        self.orm.reset()

    def test_get(self):
        self.populate()
        #: Get always returns one result using 'fetchone'
        result = self.orm.use('foo').get()
        self.assertNotEqual(result.__class__, list)

        #: Expects a :class:Query to be returned
        result = self.orm.use('foo').dry().get()
        self.assertIsInstance(result, Query)

    def test_select_cursor_factories(self):
        self.populate()
        orm = ORM(cursor_factory=CNamedTupleCursor, schema='cargo_tests')
        orm.set_table('foo')
        self.populate()
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        # Plain SELECT
        orm.use(f1.table)
        orm.where(f2 == 'bar')
        orm.limit(2)
        res = orm.select(f1, f2)
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 2)
        self.assertTrue(hasattr(res[0], '_asdict'))
        orm.close()

        orm = ORM(cursor_factory=psycopg2.extras.RealDictCursor,
                  schema='cargo_tests')
        self.populate()
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        # Plain SELECT
        orm.use(f1.table)
        orm.where(f2 == 'bar')
        orm.limit(2)
        res = orm.select(f1, f2)
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 2)
        self.assertIsInstance(res[0], dict)
        orm.close()

    def test_update(self):
        self.populate()
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        # Plain UPDATE
        self.orm.returning()
        f1.value = random.randint(1, 100000)
        f2.value = 'bar'
        res = self.orm.update(f2)
        self.assertIsInstance(res, list)
        self.assertTrue(len(res) > 0)
        self.assertEqual(len(res[0]), 2)

        # Non-executing UPDATE
        self.orm.returning()
        f1.value = random.randint(1, 100000)
        res = self.orm.dry().update(f1, f2 == 'bar')
        self.assertIsInstance(res, Query)
        self.assertFalse(len(self.orm.queries))
        self.assertFalse(len(self.orm.state.clauses))

        # Subquery UPDATE
        res = self.orm.subquery().update(f1, f2)
        self.assertIsInstance(res, Subquery)

        # Multi UPDATE
        self.orm.multi()
        self.orm.into('foo')
        res = self.orm.update(f1 == random.randint(1, 100000),
                              f2 == 'textfield')
        self.assertIs(res, self.orm)
        self.assertEqual(len(self.orm.queries), 1)
        self.orm.reset_multi()

    def test_delete(self):
        self.populate()
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        # Plain DELETE
        self.orm.use('foo')
        self.orm.where(f1 == 12345)
        self.orm.returning()
        res = self.orm.where(True).delete()
        self.assertIsInstance(res, list)
        self.assertTrue(len(res) > 0)
        self.assertEqual(len(res[0]), 2)
        with self.assertRaises(QueryError):
            self.orm.delete()

        # Non-executing DELETE
        self.orm.use('foo')
        self.orm.where(f1 == 12345)
        self.orm.returning()
        res = self.orm.dry().where(True).delete()
        self.assertIsInstance(res, Query)
        self.assertFalse(len(self.orm.queries))
        self.assertFalse(len(self.orm.state.clauses))

        # Subquery DELETE
        res = self.orm.use('foo').subquery().where(f1 == 12345).delete()
        self.assertIsInstance(res, Subquery)

        # Multi DELETE
        self.orm.multi()
        res = self.orm.use('foo').where(True).delete()
        self.assertIs(res, self.orm)
        self.assertEqual(len(self.orm.queries), 1)
        self.orm.reset_multi()

    def test_raw(self):
        self.populate()
        f1 = new_field('int', name='uid', table='foo')
        f2 = new_field('text', name='textfield', table='foo')
        # Plain NAKED SELECT
        self.orm.state.add(Clause('SELECT', safe('uid')))
        self.orm.use(f1.table)
        self.orm.where(f2 == 'bar')
        self.orm.limit(2)
        q = self.orm.dry().raw()
        self.assertEqual(q.string % q.params,
                         'SELECT uid FROM foo WHERE foo.textfield = bar ' +
                         'LIMIT 2')
        # Plain NAKED execute
        r = q.execute().fetchall()
        self.assertEqual(len(r), 2)

    def test_upsert(self):
        pass

    def test_run_iter(self):
        queries = [
            self.orm.dry().select(1),
            self.orm.dry().select(2),
            self.orm.dry().select(3),
        ]
        res = self.orm.run_iter(*queries)
        self.assertIsInstance(res, types.GeneratorType)
        self.assertEqual(len(list(res)), 3)
        nt = namedtuple('Record', ('?column?',), rename=True)
        self.assertNotEqual(list(res), [[nt(1)], [nt(2)], [nt(3)]])
        res = self.orm.run_iter(*queries[:1], fetch=True)
        self.assertIsInstance(res, types.GeneratorType)
        self.assertListEqual(list(res), [[nt(1)]])

    def test_run(self):
        queries = [
            self.orm.dry().select(1),
            self.orm.dry().select(2),
            self.orm.dry().select(3),
        ]
        res = self.orm.run(*queries)
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 3)
        nt = namedtuple('Record', ('?column?',), rename=True)
        self.assertEqual(
            res, [
                [nt(1)],
                [nt(2)],
                [nt(3)]
            ])
        res = self.orm.run(*queries[:1])
        self.assertIsInstance(res, list)
        self.assertListEqual(res, [nt(1)])

    def test_execute(self):
        qps = (
            ('SELECT %s', (1,)),
            ('SELECT %(foo)s', {'foo': 1})
        )
        for q, p in qps:
            if isinstance(p, dict):
                for x in self.orm.execute(q, p).fetchall():
                    for k, (_, v) in enumerate(p.items()):
                        self.assertEqual(getattr(x, '_' + str(k)), v)
            else:
                for x in self.orm.execute(q, p).fetchall():
                    for k, v in enumerate(p):
                        self.assertEqual(getattr(x, '_' + str(k)), v)
        with self.assertRaises(QueryError):
            self.orm.execute('SELECT ORDER').fetchall()

    def test_multi(self):
        orm = self.orm
        orm.multi()
        self.assertTrue(orm._multi)
        f1, f2 = new_field(table='foo', name='uid'),\
                 new_field('text', table='foo', name='textfield')
        orm.values(1234567, 'fish').returning()
        self.assertIs(orm.insert(f1, f2), orm)
        self.assertTrue(orm._multi)
        self.assertEqual(len(orm.queries), 1)
        orm.values(12345678, 'fish').returning()
        self.assertIs(orm.insert(f1, f2), orm)
        self.assertTrue(orm._multi)
        self.assertEqual(len(orm.queries), 2)
        self.assertIs(orm.use('foo').where(f2 == 'fish').select(), orm)
        self.assertTrue(orm._multi)
        self.assertEqual(len(orm.queries), 3)
        self.assertEqual(len(orm.run()), 3)
        self.assertFalse(orm._multi)
        # Query in the midst
        orm.multi()
        orm.values(12345679, 'fish').returning()
        orm.insert(f1, f2)
        orm.values(123456710, 'fish').returning()
        q = orm.dry().insert(f1, f2)
        res = orm.run(q)
        self.assertEqual(len(res), 1)
        self.assertTrue(orm._multi)
        res = orm.run()
        self.assertEqual(len(res), 1)
        self.assertFalse(orm._multi)

    def test_reset_multi(self):
        self.assertFalse(self.orm._multi)
        self.orm.multi()
        self.assertTrue(self.orm._multi)
        self.orm.where(1)
        self.orm.select()
        self.assertEqual(len(self.orm.queries), 1)
        self.orm.where(1)
        self.assertEqual(len(self.orm.state.clauses), 1)
        self.orm.reset_multi()
        self.assertFalse(self.orm._multi)
        self.assertEqual(len(self.orm.queries), 0)
        self.assertEqual(len(self.orm.state.clauses), 0)

    def test_add_query(self):
        self.assertEqual(len(self.orm.queries), 0)
        q = Query(orm=self.orm, query='SELECT 1')
        self.orm.add_query(q, q)
        self.assertEqual(len(self.orm.queries), 2)

    def test_state(self):
        self.assertIsInstance(self.orm.state, QueryState)

    def test_reset_state(self):
        self.orm.state.add(new_clause())
        self.assertEqual(len(self.orm.state.clauses), 1)
        self.orm.reset_state()
        self.assertEqual(len(self.orm.state.clauses), 0)

    def test_reset(self):
        self.orm.queries = [1, 2]
        self.orm._multi = True
        self.orm.state.add(new_clause())

        self.orm.reset()

        self.assertTrue(self.orm._multi)
        self.assertEqual(len(self.orm.queries), 2)
        self.assertEqual(len(self.orm.state.clauses), 0)

        self.orm.reset(True)

        self.assertFalse(self.orm._multi)
        self.assertEqual(len(self.orm.queries), 0)

    def test_copy(self):
        orm_a = ORM()
        orm_a.add_query(1, 2)
        orm_a.state.add(new_clause())
        orm_b = orm_a.copy()
        self.assertIsNot(orm_a, orm_b)
        self.assertIsNot(orm_a.queries, orm_b.queries)
        self.assertIsNot(orm_a._state, orm_b._state)
        self.assertListEqual(orm_a.queries, orm_b.queries)
        self.assertDictEqual(orm_a.state.clauses, orm_b.state.clauses)
        orm_b.state.add(new_clause('INTO'))
        orm_a.add_query(3)
        self.assertNotEqual(orm_a.queries, orm_b.queries)
        self.assertNotEqual(orm_a.state.clauses, orm_b.state.clauses)
        self.assertIsNot(orm_a.state.params, orm_b.state.params)

    def test_deepcopy(self):
        orm = copy.deepcopy(self.orm)
        for k, v in self.orm.__dict__.copy().items():
            x = getattr(orm, k)
            if k in {'schema', 'table', '_cursor_factory'}:
                self.assertEqual(v, x)
                continue
            if not isinstance(v, (type(False), type(None), type(True),
                                  self.orm.db.__class__)):
                self.assertIsNot(v, x)

    def test_pickle(self):
        b = pickle.loads(pickle.dumps(self.orm))
        for k in self.orm.__dict__:
            if k in {'_client'}:
                continue
            if isinstance(
               getattr(self.orm, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(self.orm, k), getattr(b, k))
            else:
                self.assertTrue(
                    getattr(self.orm, k).__class__ == getattr(b, k).__class__)


class TestORMRealDictCursor(TestORM):

    def setUp(self):
        self.orm = ORM(schema='cargo_tests',
                       cursor_factory=psycopg2.extras.RealDictCursor)
        local_client.clear()

    def test_execute(self):
        qps = (
            ('SELECT %s', (1,)),
            ('SELECT %(foo)s', {'foo': 1})
        )
        for q, p in qps:
            if isinstance(p, dict):
                for x in self.orm.execute(q, p).fetchall():
                    for k, (_, v) in enumerate(p.items()):
                        self.assertEqual(x['?column?'], v)
            else:
                for x in self.orm.execute(q, p).fetchall():
                    for k, v in enumerate(p):
                        self.assertEqual(x['?column?'], v)
        with self.assertRaises(QueryError):
            self.orm.execute('SELECT ORDER').fetchall()

    def test_run_iter(self):
        queries = [
            self.orm.dry().select(1),
            self.orm.dry().select(2),
            self.orm.dry().select(3),
        ]
        res = self.orm.run_iter(*queries)
        self.assertIsInstance(res, types.GeneratorType)
        self.assertEqual(len(list(res)), 3)
        self.assertNotEqual(list(res), [[{'?column?': 1}],
                                        [{'?column?': 2}],
                                        [{'?column?': 3}]])
        res = self.orm.run_iter(*queries[:1], fetch=True)
        self.assertIsInstance(res, types.GeneratorType)
        self.assertListEqual(list(res), [[{'?column?': 1}]])

    def test_run(self):
        queries = [
            self.orm.dry().select(1),
            self.orm.dry().select(2),
            self.orm.dry().select(3),
        ]
        res = self.orm.run(*queries)
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 3)
        self.assertEqual(
            res, [
                [{'?column?': 1}],
                [{'?column?': 2}],
                [{'?column?': 3}]
            ])
        res = self.orm.run(*queries[:1])
        self.assertIsInstance(res, list)
        self.assertListEqual(res, [{'?column?': 1}])


class TestORMOrderedDictCursor(TestORMRealDictCursor):

    def setUp(self):
        self.orm = ORM(schema='cargo_tests',
                       cursor_factory=OrderedDictCursor)
        local_client.clear()


class TestORMDictCursor(TestORMRealDictCursor):

    def setUp(self):
        self.orm = ORM(schema='cargo_tests',
                       cursor_factory=psycopg2.extras.DictCursor)
        local_client.clear()

    def test_run_iter(self):
        queries = [
            self.orm.dry().select(1),
            self.orm.dry().select(2),
            self.orm.dry().select(3),
        ]
        res = self.orm.run_iter(*queries)
        self.assertIsInstance(res, types.GeneratorType)
        self.assertEqual(len(list(res)), 3)
        self.assertNotEqual(list(res), [[1],
                                        [[2]],
                                        [[3]]])
        res = self.orm.run_iter(*queries[:1], fetch=True)
        self.assertIsInstance(res, types.GeneratorType)
        self.assertListEqual(list(res), [[[1]]])

    def test_run(self):
        queries = [
            self.orm.dry().select(1),
            self.orm.dry().select(2),
            self.orm.dry().select(3),
        ]
        res = self.orm.run(*queries)
        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 3)
        self.assertEqual(
            res, [
                [[1]],
                [[2]],
                [[3]]
            ])
        res = self.orm.run(*queries[:1])
        self.assertIsInstance(res, list)
        self.assertListEqual(res, [[1]])


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestORM,
                        TestORMRealDictCursor,
                        TestORMOrderedDictCursor,
                        TestORMDictCursor,
                        failfast=True,
                        verbosity=2)
