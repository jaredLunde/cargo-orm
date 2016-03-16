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

from cargo import ORM, db, create_kola_db, fields, Model
from cargo.builder import *


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestDrop(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        drop = Drop(self.orm, 'view', 'foo')
        print(drop)
        print(drop_table(self.orm, 'foo', dry=True))
        print(drop_index(self.orm, 'foo_unique_index', dry=True))
        print(create_schema(self.orm, 'foobar'))
        print(drop_schema(self.orm, 'foobar'))
        mod = Model()
        mod.table = 'foobar'
        print(drop_model(mod, dry=True))

if __name__ == '__main__':
    # Unit test
    unittest.main()
