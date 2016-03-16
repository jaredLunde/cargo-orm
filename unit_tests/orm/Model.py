#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for cargo.orm.Model`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import copy
import types
import pickle
import unittest
from random import randint
import psycopg2.extras
from collections import namedtuple, OrderedDict

from vital.debug import Timer
from vital.security import randkey
from kola import config, local, logg

from cargo import *
from cargo.orm import ORM, QueryState


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_client()


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


class BaseFoo(Model):
    uid = UID()


class Foo(BaseFoo):
    textfield = Text()
    # clone = Relationship('FooB.uid', backref='clone')


class FooB(BaseFoo):
    textfield = Text()


class FooC(Model):
    foo_a = UID()
    foo_b = Int(index=True)
    foo_c = Int(unique=True, index=True)
    foo_d = Text(unique=True, index=True)
    foo_e = Text(index=True)


class FooD(Model):
    foo_b = Int(index=True)
    foo_c = Int(unique=True, index=True)
    foo_d = Text(unique=True, index=True)
    foo_e = Text(index=True)


class FooE(Model):
    foo_b = Int(index=True)
    foo_d = Text(unique=True, index=True)
    foo_e = Text(index=True)


class FooF(Model):
    foo_b = Int(index=True)
    foo_e = Text(index=True)


class FooMultiPrimary(Model):
    table = 'foo'
    uid = UID()
    uid2 = UID()


class TestModel(unittest.TestCase):
    model = Foo()
    modelb = FooB()
    modelc = FooC()
    modeld = FooD()
    modele = FooE()
    modelf = FooF()
    model_prim = FooMultiPrimary()
    raw_model = Foo(naked=True)
    dict_model = Foo(cursor_factory=psycopg2.extras.DictCursor, naked=True)
    real_dict_model = Foo(cursor_factory=psycopg2.extras.RealDictCursor,
                          naked=True)
    o_dict_model = Foo(cursor_factory=OrderedDictCursor, naked=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test__compile(self):
        self.assertEqual(self.model.table, 'foo')
        self.assertEqual(self.modelb.table, 'foo_b')
        self.assertSequenceEqual(
            set(self.model.field_names), set(['uid', 'textfield']))
        self.assertSequenceEqual(
            set(self.modelb.field_names), set(['uid', 'textfield']))
        self.assertListEqual(self.model.relationships, [])
        self.assertListEqual(self.modelb.foreign_keys, [])
        self.assertListEqual(self.model.foreign_keys, [])
        self.assertListEqual(self.modelb.relationships, [])

    def fill(self, num=10):
        self.model.clear()
        self.model.where(True).delete()
        self.model.multi()
        for x in range(num):
            self.model.add(uid=1234567 + x, textfield='bar')
        self.model.run()

    def test___getitem__(self):
        self.model.uid.value = 12345
        self.assertEqual(self.model['uid'], self.model.uid.value)
        self.model.clear()

    def test___setitem__(self):
        self.model['uid'] = 12345
        self.assertEqual(self.model.uid.value, 12345)
        self.model.clear()

    def test___delitem__(self):
        self.model['uid'] = 12345
        self.assertEqual(self.model.uid.value, 12345)
        del self.model['uid']
        self.assertIsNone(self.model['uid'])

    def test___iter__(self):
        fields = []
        for field in self.model:
            self.assertIsInstance(field, Field)
            fields.append(field.field_name)
        self.assertSequenceEqual(set(fields), {'uid', 'textfield'})

    def test_names(self):
        self.assertSequenceEqual(
            set(self.model.names),
            set(['foo.uid', 'foo.textfield']))

    def test_field_names(self):
        self.assertSequenceEqual(
            set(self.model.field_names),
            set(['uid', 'textfield']))

    def test_field_values(self):
        self.assertSequenceEqual(
            set(self.model.field_values),
            set([Field.empty, Field.empty]))

    def test_indexes(self):
        ui = [self.modelc.foo_b, self.modelc.foo_e]
        self.assertEqual(len(self.modelc.indexes), 4)
        for idx in self.modelc.indexes:
            self.assertIn(idx, ui)

    def test_unique_indexes(self):
        ui = [self.modelc.foo_c, self.modelc.foo_d]
        self.assertEqual(len(self.modelc.unique_indexes), 2)
        for idx in self.modelc.unique_indexes:
            self.assertIn(idx, ui)

    def test_primary_key(self):
        self.assertIs(self.modelc.foo_a, self.modelc.primary_key)

    def test_best_indexes(self):
        self.model.clear()
        self.assertSequenceEqual(
            list(self.modelc.best_indexes),
            [self.modelc.foo_a, self.modelc.foo_c, self.modelc.foo_d,
             self.modelc.foo_b, self.modelc.foo_e])

    def test_best_index(self):
        self.model.clear()
        self.assertIs(self.modelc.best_index, self.modelc.primary_key)
        self.assertIs(self.modeld.best_index, self.modeld.foo_c)
        self.assertIs(self.modele.best_index, self.modele.foo_d)
        self.assertIs(self.modelf.best_index, self.modelf.foo_b)

    def test_best_available_index(self):
        self.model.clear()
        self.assertIsNone(self.model.best_available_index)
        self.assertIsNone(self.modelb.best_available_index)
        self.assertIsNone(self.modelc.best_available_index)
        self.assertIsNone(self.modeld.best_available_index)
        self.assertIsNone(self.modele.best_available_index)
        self.assertIsNone(self.modelf.best_available_index)
        for mod in (self.modelc, self.modeld, self.modele):
            _idx = None
            for idx in mod.plain_indexes:
                _idx = idx
                idx(1234)
                break
            i1 = mod.best_available_index
            i2 = (_idx == 1234)
            self.assertEqual(i1.string % i1.params, i2.string % i2.params)
            for idx in mod.unique_indexes:
                _idx = idx
                idx(12345)
                break
            ui1 = mod.best_available_index
            ui2 = (_idx == 12345)
            self.assertEqual(ui1.string % ui1.params, ui2.string % ui2.params)
            self.assertNotEqual(ui1.left.name, i1.left.name)
            if mod.primary_key is not None:
                mod.primary_key(123456)
                pk1 = mod.best_available_index
                pk2 = (mod.primary_key == 123456)
                self.assertEqual(pk1.string % pk1.params,
                                 pk2.string % pk2.params)

    def test_add(self):
        #: Expects a single result (copy of self)
        ret = self.model.add(uid=1234567, textfield='bar')
        self.assertEqual(ret.uid.value, 1234567)
        self.assertEqual(ret.textfield.value, 'bar')
        self.assertIsInstance(ret, self.model.__class__)
        self.assertIsNot(ret, self)

        #: Expects a single raw psycopg2 cursor factory
        ret = self.model.naked().add(uid=1234567, textfield='bar')
        self.assertEqual(ret.uid, 1234567)
        self.assertEqual(ret.textfield, 'bar')
        self.assertIsInstance(ret, tuple)

        #: Expects list of single results as copies of self
        self.model.multi()
        self.model.add(uid=1234567, textfield='bar')
        self.model.add(uid=1234568, textfield='bar')
        self.model.add(uid=1234569, textfield='bar')
        ret = self.model.run()
        self.assertIsInstance(ret, list)
        for r in ret:
            self.assertEqual(r.textfield.value, 'bar')
            self.assertIsInstance(r, self.model.__class__)
            self.assertIsNot(r, self)

        #: Expects list of single raw results as cursor factories
        self.model.multi()
        self.model.add(uid=1234567, textfield='bar')
        self.model.add(uid=1234568, textfield='bar')
        self.model.add(uid=1234569, textfield='bar')
        ret = self.model.naked().run()
        self.assertIsInstance(ret, list)
        for r in ret:
            self.assertEqual(r.textfield, 'bar')
            self.assertIsInstance(r, tuple)
        self.model.clear()

    def test_approx_size(self):
        self.fill(4)
        row = self.model.approx_size()
        self.assertTrue(str(row).isdigit())
        row = self.model.approx_size('num')
        self.assertTrue(str(row.num).isdigit())

    def test_exact_size(self):
        self.fill(4)
        row = self.model.exact_size()
        self.assertEqual(row, 4)
        row = self.model.exact_size('num')
        self.assertEqual(row.num, 4)

    def test_fill(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        self.assertIn(rds['uid'], self.model.values())
        self.assertIn(rds['textfield'], self.model.values())
        with self.assertRaises(KeyError):
            rds = {"foo": "bar"}
            self.model.fill(**rds)
        self.model.clear()

    def test_to_json(self):
        rds = {
            'textfield': 'foo',
            'uid': 1234
        }
        self.model.fill(**rds)
        self.assertIn(
            self.model.to_json(), (
                '{"textfield":"foo","uid":1234}',
                '{"uid":1234,"textfield":"foo"}'))
        self.model.reset()

    def test_from_json(self):
        self.model.from_json('{"uid":1234,"textfield":"foo"}')
        self.assertEqual(self.model['uid'], 1234)
        self.assertEqual(self.model['textfield'], 'foo')
        self.model.clear()

    def test_namedtuple(self):
        # To NT
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 1000000)
        }
        self.model.fill(**rds)
        self.assertEqual(self.model.to_namedtuple().uid, rds['uid'])
        self.assertEqual(self.model.to_namedtuple().textfield, rds['textfield'])

        # From NT
        nt = self.model.to_namedtuple()
        nt = nt._replace(uid=76)
        self.model.from_namedtuple(nt)
        self.assertEqual(self.model['uid'], 76)
        self.model.clear()

    def test_insert(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        #: Single
        #  Insert always returns one result (self)
        self.model.fill(**rds)
        result = self.model.insert()
        self.assertIs(self.model, result)
        self.assertEqual(result.textfield.value, rds['textfield'])
        self.assertEqual(result.uid.value, rds['uid'])
        # Single w/ returning
        self.model['uid'] = 67
        q = self.model.dry().insert(self.model.uid)
        for clause in q.evaluate_state():
            if clause.startswith('RETURNING'):
                self.assertEqual(clause, 'RETURNING foo.uid')
        self.assertIs(self.model.run(q), self.model)
        self.assertEqual(result.textfield.value, rds['textfield'])
        self.assertEqual(result.uid.value, 67)
        # Single raw
        self.model.fill(**rds)
        result = self.model.naked().insert()
        self.assertTrue(hasattr(result, '_asdict'))
        self.assertEqual(self.model.textfield.value, rds['textfield'])
        self.assertEqual(self.model.uid.value, rds['uid'])
        self.model.clear()

    def test_many(self):
        #: Many models
        self.model.many()
        for _ in range(5):
            rds = {
                'textfield': randkey(48),
                'uid': randint(1, 10000)
            }
            self.model.fill(**rds)
            self.model.insert()
        result = self.model.run()
        self.assertIsInstance(result, list)
        for x in result:
            self.assertIsInstance(x, self.model.__class__)

        #: Many Raw
        self.model.many()
        for _ in range(5):
            rds = {
                'textfield': randkey(48),
                'uid': randint(1, 10000)
            }
            self.model.fill(**rds)
            self.model.insert()
        result = self.model.naked().run()
        self.assertIsInstance(result, list)
        for x in result:
            self.assertIsInstance(x, tuple)
        self.model.clear()

    def test_update(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        self.model.insert()
        self.model['textfield'] = 'bar'
        #: Test single field update
        q = self.model.dry().update(self.model.textfield)
        self.assertEqual(q.query % q.params,
                         ('UPDATE foo SET textfield = bar WHERE foo.uid = {}' +
                          ' RETURNING foo.textfield').format(rds['uid']))
        #: Multi-field update
        q = self.model.dry().update(self.model.textfield, self.model.uid)
        self.assertEqual(q.query % q.params,
                         ('UPDATE foo SET textfield = bar, uid = {uid} ' +
                          'WHERE foo.uid = {uid} ' +
                          'RETURNING foo.textfield, foo.uid'
                          ).format(uid=rds['uid']))
        #: Update all
        q = self.model.dry().update()
        self.assertIn(q.query % q.params, [
                      ('UPDATE foo SET textfield = bar, uid = {uid} ' +
                       'WHERE foo.uid = {uid}' +
                       ' RETURNING *'
                       ).format(uid=rds['uid']), (
                        'UPDATE foo SET uid = {uid}, textfield = bar ' +
                        'WHERE foo.uid = {uid}' +
                        ' RETURNING *'
                        ).format(uid=rds['uid']),
                      ])
        # Updates return lists by default
        result = self.model.update()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], self.model.__class__)
        result = self.model.naked().update()
        self.assertTrue(hasattr(result[0], '_asdict'))
        # Update one returns self
        result = self.model.one().update()
        self.assertIs(result, self.model)
        result = self.model.naked().update()
        self.assertTrue(hasattr(result[0], '_asdict'))
        self.model.clear()
        #: With no model info, update should raise ORMIndexError if
        #  no explicit WHERE clause was specified
        with self.assertRaises(ORMIndexError):
            self.model.update()
        self.assertIs(
            self.model,
            self.model.where(self.model.uid == rds['uid'])
                      .set(self.model.textfield.eq('foobar'))
                      .one()
                      .update())
        self.model.clear()

    def test_save(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        # Insert expects one result returned (self)
        q = self.model.dry().save()
        self.assertEqual(q.__querytype__, 'INSERT')
        result = self.model.run(q)
        self.assertTrue(q.one)
        self.assertIs(self.model, result)
        self.assertEqual(result.textfield.value, rds['textfield'])
        self.assertEqual(result.uid.value, rds['uid'])
        self.model['textfield'] = 'bar'
        # Update expects one result returned (self)
        q = self.model.dry().save()
        result = self.model.run(q)
        self.assertTrue(q.one)
        self.assertIs(self.model, result)
        self.assertEqual(result.textfield.value, 'bar')
        self.assertEqual(result.uid.value, rds['uid'])
        self.assertEqual(q.__querytype__, 'UPDATE')
        # Update expects one result returned (cursor factory)
        result = self.model.naked().save()
        self.assertTrue(hasattr(result, '_asdict'))
        self.assertEqual(result.textfield, 'bar')
        self.assertEqual(result.uid, rds['uid'])
        self.model.clear()
        #: With no model info, update should raise ORMIndexError if
        #  no explicit WHERE clause was specified
        with self.assertRaises(ORMIndexError):
            del self.model['uid']
            self.model.save()

    def test_select(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        self.model.save()
        #: Select always returns a list if the query isn't dry
        #  By default, results are casted to models which are copies of the
        #  current one.
        result = self.model.select()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], self.model.__class__)
        #: Expects list of raw cursor factories
        result = self.model.naked().select()
        self.assertIsInstance(result, list)
        self.assertTrue(hasattr(result[0], '_asdict'))
        #: Expects :class:Query
        result = self.model.naked().dry().select()
        self.assertIsInstance(result, Query)
        self.model.clear()

    def test_get(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        #: Get always returns a single result
        self.model.save()
        #: Get always returns one result (self by default)
        result = self.model.get()
        self.assertIs(result, self.model)
        #: Expects :class:Query
        result = self.model.dry().get()
        self.assertIsInstance(result, Query)
        #: Expects raw cursor factory
        result = self.model.naked().get()
        self.assertTrue(hasattr(result, '_asdict'))
        self.model.clear()

    def test_delete(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        self.model.save()
        #: Delete always returns a list
        result = self.model.dry().delete()
        self.assertIsInstance(result, Query)
        #: Delete always returns a list when running
        result = self.model.delete()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], self.model.__class__)
        self.assertEqual(self.model.uid.value, result[0].uid.value)

        self.model.fill(**rds)
        self.model.save()
        #: Expects list of raw cursor factories
        result = self.model.naked().delete()
        self.assertIsInstance(result, list)
        self.assertTrue(hasattr(result[0], '_asdict'))
        self.assertEqual(self.model.uid.value, result[0].uid)
        self.model.clear()
        #: With no model info, update should raise ORMIndexError if
        #  no explicit WHERE clause was specified
        with self.assertRaises(ORMIndexError):
            self.model.delete()

    def test_remove(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        self.model.save()
        #: Remove always returns a single result (self) and clears itself
        result = self.model.remove()
        self.assertIs(self.model.uid.value, self.model.uid.empty)
        self.assertIs(self.model.textfield.value, self.model.textfield.empty)
        self.assertIs(result, self.model)

        self.model.fill(**rds)
        self.model.save()
        #: Expects a single raw cursor factory
        result = self.model.naked().remove()
        self.assertTrue(hasattr(result, '_asdict'))
        self.model.clear()
        #: With no model info, update should raise ORMIndexError if
        #  no explicit WHERE clause was specified
        with self.assertRaises(ORMIndexError):
            self.model.delete()

    def test_pop(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        self.model.save()
        #: Expects a single result as a copy of self
        result = self.model.pop()
        self.assertIsNone(self.model.get())
        self.assertIsInstance(result, self.model.__class__)
        self.assertEqual(rds['textfield'], result.textfield.value)
        self.assertEqual(rds['uid'], result.uid.value)

        self.model.fill(**rds)
        self.model.save()
        #: Expects a single result as cursor factory
        result = self.model.naked().pop()
        self.assertIsNone(self.model.get())
        self.assertTrue(hasattr(result, '_asdict'))
        self.assertEqual(rds['textfield'], result.textfield)
        self.assertEqual(rds['uid'], result.uid)

        self.model.fill(**rds)
        self.model.save()
        #: Expects a single :class:Query
        result = self.model.dry().pop()
        self.assertIsNotNone(self.model.get())
        self.assertIsInstance(result, Query)
        self.model.clear()

    def test_multi_orm(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        rds_b = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds).save()
        q1 = self.model.dry().get()
        self.assertIsInstance(q1, Query)
        self.assertEqual(q1.__querytype__, 'SELECT')
        q2 = self.modelb.dry().get()
        result = self.model.multi(q1, q2).run()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], self.model.__class__)
        self.assertIsInstance(result[1], self.model.__class__)
        self.model.clear()
        self.modelb.clear()

    def test_multi_many_orm(self):
        self.model.many()
        self.modelb.many()
        for x in range(10):
            rds = {
                'textfield': randkey(48),
                'uid': randint(1, 10000)
            }
            rds_b = {
                'textfield': randkey(48),
                'uid': randint(1, 10000)
            }
            self.model.fill(**rds).insert()
            self.modelb.fill(**rds_b).insert()
        q1 = self.model.end_many()
        q2 = self.modelb.end_many()
        result = self.model.multi().add_query(q1, q2).run()
        for q in result:
            for r in q:
                self.assertIsInstance(r, self.model.__class__)
        self.assertEqual(len(result[0]), 10)
        self.assertEqual(len(result[1]), 10)
        result = self.model.multi(q1, q2).run()
        for q in result:
            for r in q:
                self.assertIsInstance(r, self.model.__class__)
        self.assertEqual(len(result[0]), 10)
        self.assertEqual(len(result[1]), 10)
        self.model.clear()
        self.modelb.clear()

    def test_join(self):
        s = self.model.join(self.modelb)
        self.assertIs(s, self.model)
        self.assertTrue(self.model.state.has('JOIN'))
        clause = self.model.state.clauses.popitem(last=True)[1][0]
        self.assertEqual(clause.string, 'JOIN foo_b ON foo_b.uid = foo.uid')
        self.model.clear()

    def test_iternaked(self):
        self.fill(10)
        raws = []
        for x in self.model.iternaked():
            self.assertTrue(hasattr(x, '_asdict'))
            raws.append(x)
        self.assertEqual(len(raws), 10)
        self.model.clear()

    def test_iter(self):
        self.fill(10)
        res = []
        for x in self.model.iter():
            self.assertIsInstance(x, self.model.__class__)
            res.append(x)
        self.assertEqual(len(res), 10)

        for i, x in enumerate(self.model.naked().iter(reverse=True), 1):
            self.assertEqual(res[i*-1].uid.value, x.uid)

        res2 = []
        for i, x in enumerate(self.model.iter(offset=2, limit=2)):
            self.assertEqual(x.uid.value, res[2+i].uid.value)
            res2.append(x)
        self.assertEqual(len(res2), 2)

        res3 = []
        for x in self.model.iter(buffer=1):
            res3.append(x)
        self.assertEqual(len(res3), 10)

        res4 = []
        for x in self.model.where(self.model.uid > res[1].uid()).iter():
            res4.append(x)
        self.assertEqual(len(res4), 8)
        self.model.clear()

    def test_reset_fields(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.fill(**rds)
        for field in self.model:
            self.assertIn(field.field_name, rds)
            self.assertIn(field.value, rds.values())
        self.model.reset_fields()
        for field in self.model:
            self.assertIs(field.value, Field.empty)

    def test_reset(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.where(True)
        self.model.fill(**rds)
        self.assertTrue(self.model.state.has('WHERE'))
        for field in self.model:
            self.assertIn(field.field_name, rds)
            self.assertIn(field.value, rds.values())
        self.model.reset()
        for field in self.model:
            self.assertIn(field.value, rds.values())
        self.assertFalse(self.model.state.has('WHERE'))

    def test_clear(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.model.where(True)
        self.model.fill(**rds)
        self.assertTrue(self.model.state.has('WHERE'))
        for field in self.model:
            self.assertIn(field.field_name, rds)
            self.assertIn(field.value, rds.values())
        self.model.clear()
        for field in self.model:
            self.assertNotIn(field.value, rds.values())
        self.assertFalse(self.model.state.has('WHERE'))

    def test_set_table(self):
        self.model.set_table('foobar')
        for field in self.model.fields:
            self.assertEqual(field.table, 'foobar')
        self.assertEqual(self.model.table, 'foobar')
        self.model.set_table('foo')

    def test_set_alias(self):
        self.model.set_alias('foobar')
        for field in self.model.fields:
            self.assertEqual(aliased(field), 'foobar.' + field.field_name)
        self.assertEqual(aliased(self.model), 'foobar')

    def test_multi_primary(self):
        model = self.model_prim
        self.assertEqual(model.table, 'foo')
        model['uid'] = 1
        model['uid2'] = None
        self.assertIsNone(model.best_available_index)
        model['uid2'] = 2
        exp = model.best_available_index
        self.assertIn(exp.string % exp.params,
                      {'foo.uid = 1 AND foo.uid2 = 2',
                       'foo.uid2 = 2 AND foo.uid = 1'})
        self.assertSequenceEqual(set((model.uid.name, model.uid2.name)),
                                 set([field.name
                                      for field in model.primary_key]))
        exp = model.best_unique_index
        self.assertIn(exp.string % exp.params,
                      {'foo.uid = 1 AND foo.uid2 = 2',
                       'foo.uid2 = 2 AND foo.uid = 1'})

    def test_raw(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.raw_model.fill(**rds)
        result = self.raw_model.save()
        self.assertTrue(hasattr(result, '_asdict'))
        result = self.raw_model.models().save()
        self.assertIs(result, self.raw_model)

    def test_dict(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.dict_model.fill(**rds)
        result = self.dict_model.save()
        self.assertTrue(hasattr(result, 'items'))
        result = self.dict_model.models().save()
        #self.assertIs(result, self.dict_model)
        #self.dict_model.clear()
        result = self.real_dict_model.limit(3).select()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], dict)

    def test_real_dict(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.real_dict_model.fill(**rds)
        result = self.real_dict_model.save()
        self.assertTrue(hasattr(result, 'items'))
        result = self.real_dict_model.models().save()
        self.assertIs(result, self.real_dict_model)
        self.real_dict_model.clear()
        result = self.real_dict_model.limit(3).select()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], dict)

    def test_o_dict(self):
        rds = {
            'textfield': randkey(48),
            'uid': randint(1, 10000)
        }
        self.o_dict_model.fill(**rds)
        result = self.o_dict_model.save()
        self.assertTrue(hasattr(result, 'items'))
        result = self.o_dict_model.models().save()
        self.assertIs(result, self.o_dict_model)
        self.o_dict_model.clear()
        result = self.o_dict_model.limit(3).select()
        self.assertIsInstance(result, list)
        self.assertIsInstance(result[0], OrderedDict)

    def test_copy(self):
        orm_a = Foo()
        orm_a.add_query(1, 2)
        orm_a.state.add(new_clause())
        orm_b = orm_a.copy()
        self.assertIsNot(orm_a, orm_b)
        for field in orm_a:
            self.assertIsNot(field, getattr(orm_b, field.field_name))
        self.assertIsNot(orm_a.queries, orm_b.queries)
        self.assertIsNot(orm_a._state, orm_b._state)
        self.assertListEqual(orm_a.queries, orm_b.queries)
        self.assertDictEqual(orm_a.state.clauses, orm_b.state.clauses)
        orm_b.state.add(new_clause('INTO'))
        orm_a.add_query(3)
        self.assertIsNot(orm_a.queries, orm_b.queries)
        self.assertNotEqual(orm_a.state.clauses, orm_b.state.clauses)
        self.assertIsNot(orm_a.state.params, orm_b.state.params)
        self.assertIsNot(orm_a.fields, orm_b.fields)

    def test_deepcopy(self):
        orm = copy.deepcopy(self.model)
        for k, v in self.model.__dict__.items():
            x = getattr(orm, k)
            if k not in ('unique_indexes', 'indexes', 'foreign_keys',
                         'field_names', 'names', 'FooRecord') \
               and not isinstance(v, (type(False), type(None), type(True),
                                      str, self.model.db.__class__)):
                self.assertIsNot(v, x)

    def test_pickle(self):
        b = pickle.loads(pickle.dumps(self.model))
        for k in self.model.__dict__:
            if k == '_client':
                continue
            if isinstance(
               getattr(self.model, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(self.model, k), getattr(b, k))
            else:
                if k == 'FooRecord':
                    #: Ignore lazy namedtuple
                    continue
                self.assertTrue(
                    getattr(self.model, k).__class__ == getattr(b, k).__class__)


if __name__ == '__main__':
    # Unit test
    unittest.main()
