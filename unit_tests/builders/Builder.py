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
from bloom.builder import Builder, Build


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_client()


class FooUsers(Model):
    uid = UID()
    username = Username(maxlen=100, unique=True)
    password = Password(minlen=8)


class FooPosts(Model):
    ordinal = ('id', 'title', 'content', 'tags', 'slug', 'posted_on')
    id = Serial()
    title = Varchar(maxlen=500)
    content = Text()
    tags = Array(cast=str, index=True)
    slug = Slug(unique=True)
    posted_on = Timestamp(index=True)


class FooComments(Model):
    uid = UID()
    actor = ForeignKey('FooUsers.uid', unique=True)
    target = ForeignKey('FooPosts.id', unique=True)
    content = Text()
    posted_on = Timestamp(index=True)


class FooUsersBuilder(Builder):
    ordinal = ('uid', 'username', 'password')
    model = FooUsers()


class FooPostsBuilder(Builder):
    model = FooPosts()

    def before(self):
        self.constraints(check="foo > bar")
        self.timing('INITIALLY BEFORE')
        self.columns.id.set_type('int8')


class FooCommentsBuilder(Builder):
    ordinal = ('uid', 'actor', 'target', 'content', 'posted_on')
    model = FooComments()


class TestBuilder(unittest.TestCase):

    def test_builder_a(self):
        b = FooUsersBuilder()
        logg(b.model).log()
        logg(b.name).log('TABLE')
        logg(b.columns).log('COLUMNS')
        logg(b.indexes).log('INDEXES')
        b.run()
        logg(b).log()

    def test_builder_b(self):
        b = FooPostsBuilder()
        logg(b.model).log()
        logg(b.name).log('TABLE')
        logg(b.columns).log('COLUMNS')
        logg(b.indexes).log('INDEXES')
        b.run()
        logg(b).log()

    def test_builder_c(self):
        b = FooCommentsBuilder()
        logg(b.model).log()
        logg(b.name).log('TABLE')
        logg(b.columns).log('COLUMNS')
        logg(b.indexes).log('INDEXES')
        b.run()
        logg(b).log()


if __name__ == '__main__':
    # Unit test
    unittest.main()
