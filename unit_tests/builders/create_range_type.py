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
from bloom.builder import create_range_type
from bloom.builder.types import RangeType


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(range_type='char', value=None, name=None, table=None):
    field = getattr(fields, range_type.title())(value=value)
    field.field_name = name or randkey(24, keyspace='aeioughrstlnmy')
    field.table = table or randkey(24, keyspace='aeioughrstlnmy')
    return field


class TestCreateRangeType(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        range_type = create_range_type(self.orm,
                                       'foo',
                                       dry=True,
                                       subtype='foobar')
        print(range_type.query.mogrified)

        range_type = RangeType(self.orm, 'foo')
        range_type.options(input=Function('foo_in').func,
                           output=Function('foo_out').func,
                           internallength=16,
                           element='float4')
        print(range_type.query)
        print(range_type.query.mogrified)

        range_type = RangeType(self.orm,
                               'foo',
                               Clause('PASSEDBYVALUE'),
                               input=Function('foo_in').func,
                               output=Function('foo_out').func,
                               internallength=16,
                               element='float4')
        print(range_type.query)
        print(range_type.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
