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
from cargo.builder import create_index
from cargo.builder.indexes import Index


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateIndex(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        index = create_index(self.orm, 'bar', table='foo', dry=True)
        print(index.query.mogrified)
        field = new_field('int', table='foo', name='bar')
        index = Index(self.orm,
                      field,
                      partial=(field > 1000))
        (index.
         concurrent().
         unique().
         nulls('LAST').
         asc().
         fillfactor(65).
         fastupdate().
         buffering(False).
         set_type('gin'))
        print(index.query)


if __name__ == '__main__':
    # Unit test
    unittest.main()
