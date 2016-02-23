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

from bloom import ORM, db, create_kola_db, fields, Function, Clause
from bloom.builder import create_type
from bloom.builder.types import Type


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24, keyspace='aeioughrstlnmy')
    field.table = table or randkey(24, keyspace='aeioughrstlnmy')
    return field


class TestCreateType(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        attrs = ((new_field(), Clause('COLLATE', 'LATIN1')),
                 (new_field(), Clause('COLLATE', 'LATIN1')),)
        type = create_type(self.orm, 'foo', attrs=attrs, dry=True)
        print(type.query.mogrified)

        type = Type(self.orm, 'foo')
        type.options(input=Function('foo_in').func,
                     output=Function('foo_out').func,
                     internallength=16,
                     element='float4')
        print(type.query)
        print(type.query.mogrified)

        type = Type(self.orm,
                    'foo',
                    Clause('PASSEDBYVALUE'),
                    input=Function('foo_in').func,
                    output=Function('foo_out').func,
                    internallength=16,
                    element='float4')
        print(type.query)
        print(type.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
