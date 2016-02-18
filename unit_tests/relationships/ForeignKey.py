#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for vital.sql.relationships.Relationship`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import copy
import pickle
import unittest
from vital import config

from vital.sql import Model, create_vital_pool, UID, RelationshipImportError,\
                      Field
from vital.sql.relationships import Relationship, ForeignKey


config.bind('/home/jared/apps/xfaps/vital.json')
create_vital_pool()


class Foo(Model):
    uid = UID()
    b = Relationship('FooB.owner')


class FooB(Model):
    uid = UID()
    owner = ForeignKey('Foo.uid')


class FooC(Model):
    uid = UID()
    owner = ForeignKey('Foo.uid', relation='videos')


class FooD(Model):
    uid = UID()
    owner = ForeignKey('Foo.uids')


class TestForeignKey(unittest.TestCase):

    def test___init__(self):
        model = FooB()
        self.assertIsInstance(model.owner, Field)
        self.assertIsInstance(model.owner.ref.field, Foo.uid.__class__)
        self.assertIsInstance(model.owner.ref.model, Foo)
        with self.assertRaises(RelationshipImportError):
            FooD()

    def test_relation(self):
        FooC()
        model = Foo()
        self.assertIsInstance(model.videos._model, FooC)

    def test_join(self):
        model = Foo()
        modelb = FooB()
        join_types = (
            modelb.join, modelb.left_join, modelb.right_join,
            modelb.full_join, modelb.cross_join
        )
        for join in join_types:
            _args = (
                #: ON foreign key
                ([modelb.owner], {},
                 '{} foo_b ON foo_b.owner = foo.uid'),
                #: ON foreign key w/ alias
                ([modelb.owner], {'alias': 'foo_alias'},
                 '{} foo_b foo_alias ON foo_alias.owner = foo.uid'),
                #: ON model foreign
                ([modelb], {},
                 '{} foo_b ON foo_b.owner = foo.uid'),
                #: ON model foreign w/ alias
                ([modelb], {'alias': 'foo_alias'},
                 '{} foo_b foo_alias ON foo_alias.owner = foo.uid')
            )
            for args, kwargs, validate in _args:
                s = model.join(*args, **kwargs)
                clause = model.state.clauses.popitem(last=True)[1][0]
                self.assertIs(s, model)
                self.assertEqual(str(clause), validate.format(clause.clause))

    def test_copy(self):
        model = FooB()
        modelb = FooB()
        rel = model.owner
        relb = modelb.owner
        self.assertIsNot(rel, relb)

    def test_deepcopy(self):
        model = FooB()
        rel = model.owner
        relb = copy.deepcopy(rel)
        self.assertIsNot(rel, relb)

    def test_pickle(self):
        model = FooB()
        b = pickle.loads(pickle.dumps(model))
        self.assertEqual(len(model.foreign_keys), len(b.foreign_keys))
        self.assertEqual(b.owner.sqltype, model.owner.sqltype)
        self.assertIsInstance(b.owner.ref.field,
                              model.owner.ref.field.__class__)
        self.assertIsInstance(b.owner.ref.model,
                              model.owner.ref.model.__class__)
        self.assertEqual(b.owner.ref.model.table, model.owner.ref.model.table)


if __name__ == '__main__':
    # Unit test
    unittest.main()
