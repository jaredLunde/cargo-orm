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

from cargo import ORM, db, fields, Function, Clause
from cargo.builder import create_table
from cargo.builder.tables import Table


def new_field(type='char', value=None, name=None, table=None, **attrs):
    field = getattr(fields, type.title())(value=value, **attrs)
    field.field_name = name or randkey(24, keyspace='aeioughrstlnmy')
    field.table = table or randkey(24, keyspace='aeioughrstlnmy')
    return field


class TestCreateTable(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        '''table = create_table(self.orm,
                                 'foo',
                                 'BEFORE',
                                 'INSERT',
                                 Clause('UPDATE OF', new_field()),
                                 table='foo',
                                 function=Function('log_something'),
                                 dry=True)
        print(table.query.mogrified)'''

        fielda = new_field('int', table='foo', name='bar_a', primary=True)
        fieldb = new_field('varchar', table='foo', name='bar_b', maxlen=20,
                           unique=True)
        fieldc = new_field('text', table='foo', name='bar_c')

        table = Table(self.orm, 'foo')
        table.temporary()
        table.foreign_key((fielda, fieldb), 'foo_b', ('bar_a', 'bar_b'),
                          on_delete="cascade", on_update="cascade")
        table.set_columns(id=('integer', 'NOT NULL', 'PRIMARY KEY'))
        print(table.query)
        print(table.query.mogrified)

        table = Table(self.orm, 'foo')
        table.from_fields(fielda, fieldb, fieldc)
        table.constraints('DEFERRABLE', check=(fielda > 10),
                          initially_immediate=True)
        print(table.query)
        print(table.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
