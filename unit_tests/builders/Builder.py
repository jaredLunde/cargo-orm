#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for bloom.build.Plan`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from vital.debug import logg

from bloom import db, Model, ForeignKey, create_db
from bloom.fields import *
from bloom.builder import *
from bloom.builder.schemas import Schema


create_db()


class FooUsers(Model):
    uid = UID()
    username = Username(maxlen=25, unique=True, not_null=True)
    password = Password(minlen=8, not_null=True)


class FooPosts(Model):
    ordinal = ('id', 'title', 'content', 'tags', 'slug', 'score', 'posted_on')
    id = Serial()
    title = Varchar(maxlen=500)
    content = Text()
    tags = Array(type=Text(), dimensions=3, maxlen=10, index=True)
    score = Double()
    slug = Slug(unique=True, index=True)
    posted_on = Timestamp(index=True)


class FooComments(Model):
    uid = UID()
    actor = ForeignKey('FooUsers.uid', index=True, unique=False, not_null=True)
    target = ForeignKey('FooPosts.id', index=True, not_null=True)
    content = Text(not_null=True)
    posted_on = Timestamp(index=True, default=Timestamp.now())


class FooUsersPlan(Plan):
    ordinal = ('uid', 'username', 'password')
    model = FooUsers(schema='foo_0', debug=False)

    def before(self):
        self.not_exists()


class FooPostsPlan(Plan):
    model = FooPosts(schema='foo_0', debug=False)

    def before(self):
        self.columns.id.set_type('int8')
        self.not_exists()


class FooCommentsPlan(Plan):
    ordinal = ('uid', 'actor', 'target', 'content', 'posted_on')
    model = FooComments(schema='foo_0', debug=False)

    def before(self):
        self.columns.actor.unique(None)
        self.model.actor.ref.on_update('CASCADE').on_delete('CASCADE')
        self.columns.actor.references(self.model.actor.ref)
        self.columns.target = Column(self.model.target, unique=None)
        self.unique((self.model.actor, self.model.target))
        self.indexes.actor_target = Index(self.model,
                                          self.model.actor,
                                          self.model.target,
                                          partial=(self.model.target > 5),
                                          unique=True)
        del self.indexes.actor
        del self.indexes.posted_on
        self.indexes.add('posted_on', self.model.posted_on)
        self.indexes.remove('posted_on')
        self.indexes.add('posted_on', self.model.posted_on)
        self.comments.add('good one', self.columns.posted_on, 'test it')
        self.not_exists()

    def after(self):
        self.comment_on(self.columns.actor, 'Actor unique identifier from foo')
        self.comment_on(Schema(self.orm, self.schema), 'Shard 0 schema')


class TestPlan(unittest.TestCase):

    def test_builder_a(self):
        b = FooUsersPlan()
        b.debug()
        b.execute()

    def test_builder_b(self):
        b = FooPostsPlan()
        b.execute()

    def test_builder_c(self):
        b = FooCommentsPlan()
        b.execute()


if __name__ == '__main__':
    # Unit test
    # unittest.main()
    ordinal = FooUsersPlan(), FooPostsPlan(), FooCommentsPlan()
    Build(*ordinal).debug().run()
    drop_schema(db, 'foo_0', cascade=True, if_exists=True)
