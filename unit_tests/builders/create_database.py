#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.builder.create_user`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from vital.security import randkey

from cargo import ORM, db, fields
from cargo.builder import create_database
from cargo.builder.databases import Database


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
