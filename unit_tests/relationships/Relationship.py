#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.relationships.Relationship`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import copy
import pickle
import unittest
from kola import config

from bloom import Model, create_kola_pool, UID, RelationshipImportError,\
                      Field, PullError
from bloom.relationships import Relationship, ForeignKey


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_pool()


class Foo(Model):
    uid = UID()
    b = Relationship('FooB.owner')


class FooB(Model):
    uid = UID()
    owner = ForeignKey('Foo.uid')


class FooC(Model):
    uid = UID()
    b = Relationship('FooB.owners')


class FooD(Model):
    uid = UID()
    b = ForeignKey('Foo.uid', relation='videos')


class TestRelationship(unittest.TestCase):

    def test___init__(self):
        model, model2 = Foo(), Foo()
        self.assertIsInstance(model.b._model, FooB)
        self.assertIsInstance(model.b.foreign_key, Field)
        with self.assertRaises(RelationshipImportError):
            modelc = FooC()
            modelc.b.foreign_key
        model.b['uid'] = 1234
        self.assertNotEqual(model.b['uid'], model2.b['uid'])
        modelb = FooB()
        modelb2 = FooB()

        self.assertIsNot(modelb.owner, model.b.owner)
        self.assertIsNot(modelb.owner, modelb2.owner)

        with self.assertRaises(AttributeError):
            model.videos

    def test_relation(self):
        FooD()
        model = Foo()
        self.assertIsInstance(model.videos._model, FooD)

    def test_pull(self):
        model = Foo()
        model['uid'] = 6719
        model.b.offset(3).limit(1).order_by(model.b.uid.desc())
        q = model.b.pull(model.b.uid, run=False)
        q2 = model.b.pull(
            model.b.uid, offset=3, limit=1, order_field=model.b.uid,
            reverse=True, run=False)
        self.assertEqual(
            q.query % q.params,
            q2.query % q2.params)
        self.assertEqual(
            q.query % q.params,
            'SELECT foo_b.uid FROM foo_b WHERE foo_b.owner = 6719 ' +
            'ORDER BY foo_b.uid DESC LIMIT 1 OFFSET 3')
        q = model.pull(run=False)
        self.assertIsInstance(q, list)
        self.assertEqual(len(q), 1)
        self.assertEqual(
            q[0].query % q[0].params,
            "SELECT * FROM foo_b WHERE foo_b.owner = 6719")
        model.uid.clear()
        with self.assertRaises(PullError):
            model.b.pull(run=False)

    def test_join(self):
        model = Foo()
        modelb = FooB()
        join_types = (
            modelb.join, modelb.left_join, modelb.right_join,
            modelb.full_join, modelb.cross_join
        )
        for join in join_types:
            _args = (
                #: ON relationship
                ([model.b], {},
                 '{} foo_b ON foo_b.owner = foo.uid'),
                #: ON relationship w/ alias
                ([model.b], {'alias': 'foo_alias'},
                 '{} foo_b foo_alias ON foo_alias.owner = foo.uid'),
            )
            for args, kwargs, validate in _args:
                s = model.join(*args, **kwargs)
                clause = model.state.clauses.popitem(last=True)[1][0]
                self.assertIs(s, model)
                self.assertEqual(str(clause), validate.format(clause.clause))

        for join in join_types:
            _args = (
                #: ON model relationship
                ([model], {},
                 '{} foo ON foo.uid = foo_b.owner'),
                #: ON model relationship w/ alias
                ([model], {'alias': 'foo_alias'},
                 '{} foo foo_alias ON foo_alias.uid = foo_b.owner')
            )
            for args, kwargs, validate in _args:
                s = modelb.join(*args, **kwargs)
                clause = modelb.state.clauses.popitem(last=True)[1][0]
                self.assertIs(s, modelb)
                self.assertEqual(str(clause), validate.format(clause.clause))

    def test_copy(self):
        model = Foo()
        modelb = Foo()
        rel = model.b
        relb = modelb.b
        self.assertIsNot(rel, relb)
        self.assertIsNot(rel.queries, relb.queries)
        self.assertListEqual(rel.queries, relb.queries)
        self.assertDictEqual(rel.state.clauses, relb.state.clauses)
        relb.values(1, 2, 3)
        rel.add_query(3)
        self.assertNotEqual(rel.queries, relb.queries)
        self.assertNotEqual(rel.state.clauses, relb.state.clauses)
        self.assertNotEqual(rel.state.params, relb.state.params)

    def test_deepcopy(self):
        model = Foo()
        rel = model.b
        relb = copy.deepcopy(rel)
        self.assertIsNot(rel, relb)
        self.assertIsNot(rel.queries, relb.queries)
        self.assertListEqual(rel.queries, relb.queries)
        self.assertDictEqual(rel.state.clauses, relb.state.clauses)
        relb.values(1, 2, 3)
        rel.add_query(3)
        self.assertNotEqual(rel.queries, relb.queries)
        self.assertNotEqual(rel.state.clauses, relb.state.clauses)
        self.assertNotEqual(rel.state.params, relb.state.params)

    def test_pickle(self):
        model = Foo()
        b = pickle.loads(pickle.dumps(model))
        self.assertEqual(len(model.relationships), len(b.relationships))
        self.assertIsInstance(b.b, model.b.__class__)
        self.assertIsInstance(b.b._model, model.b._model.__class__)


if __name__ == '__main__':
    # Unit test
    unittest.main()
