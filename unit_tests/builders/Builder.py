#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for bloom.build.Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from vital.debug import logg

from kola import config
from bloom import ORM, Model, ForeignKey, create_kola_client
from bloom.fields import *
from bloom.builder import Builder, Build, Column, Index


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_client()


class FooUsers(Model):
    uid = UID()
    username = Username(maxlen=100, unique=True, not_null=True)
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


class FooUsersBuilder(Builder):
    ordinal = ('uid', 'username', 'password')
    model = FooUsers(debug=True)


class FooPostsBuilder(Builder):
    model = FooPosts(debug=True)

    def before(self):
        self.columns.id.set_type('int8')


class FooCommentsBuilder(Builder):
    ordinal = ('uid', 'actor', 'target', 'content', 'posted_on')
    model = FooComments(debug=True)

    def before(self):
        self.columns.actor.unique(None)
        self.columns.actor.references(None)
        self.columns.target = Column(self.model.target, unique=None)
        self.unique((self.model.actor, self.model.target))
        self.indexes.target = Index(self.model,
                                    self.model.actor,
                                    self.model.target,
                                    partial=(self.model.target > 5),
                                    unique=True)
        self.indexes.actor = Index(self.model, self.model.actor)
        del self.indexes.posted_on
        self.indexes.add('posted_on', self.model.posted_on)
        self.indexes.remove('posted_on')
        self.indexes.add('posted_on', self.model.posted_on)


class TestBuilder(unittest.TestCase):

    def test_builder_a(self):
        b = FooUsersBuilder()
        b.run()

    def test_builder_b(self):
        b = FooPostsBuilder()
        b.run()

    def test_builder_c(self):
        b = FooCommentsBuilder()
        b.run()


if __name__ == '__main__':
    # Unit test
    unittest.main()
