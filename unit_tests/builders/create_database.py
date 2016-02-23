#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.builder.create_user`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from kola import config
from vital.security import randkey

from bloom import ORM, db, create_kola_db, fields
from bloom.builder import create_database
from bloom.builder.databases import Database


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateDatabase(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        database = create_database(self.orm,
                                   'foo',
                                   owner='prototype',
                                   dry=True)
        print(database.query.mogrified)
        field = new_field('int', table='foo', name='bar')
        database = Database(self.orm, 'foo')
        database.encoding('LATIN1')
        database.tablespace('foobar')
        database.connlimit(1000)
        print(database.query)
        print(database.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
