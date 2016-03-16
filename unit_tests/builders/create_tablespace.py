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

from kola import config
from vital.security import randkey

from cargo import ORM, db, create_kola_db, fields
from cargo.builder import create_tablespace
from cargo.builder.tablespaces import Tablespace


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateTablespace(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        tablespace = create_tablespace(self.orm, 'foo', '/data/foo', dry=True)
        print(tablespace.query.mogrified)
        tablespace = Tablespace(self.orm, 'foo', '/data/foo')
        tablespace.owner('jared')
        tablespace.options(random_page_cost=4.0, seq_page_cost=1.0)
        print(tablespace.query)
        print(tablespace.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
