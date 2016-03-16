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
from cargo.builder import create_cast
from cargo.builder.casts import Cast


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateCast(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        cast = create_cast(self.orm, 'bigint', 'int8', dry=True)
        print(cast.query)
        field = new_field('int', table='foo', name='bar')
        cast = Cast(self.orm, 'citext', 'text')
        cast.as_assignment()
        cast.as_implicit()
        cast.inout()
        print(cast.query)


if __name__ == '__main__':
    # Unit test
    unittest.main()
