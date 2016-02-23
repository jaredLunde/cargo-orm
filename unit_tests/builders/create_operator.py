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
from bloom.builder import create_operator
from bloom.builder.operators import Operator


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateOperator(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        operator = create_operator(self.orm, '''~+~''', 'int8', dry=True)
        print(operator.query.mogrified)
        operator = Operator(self.orm, '~-~', 'int8')
        operator.opts(LEFTARG='foo', rightarg='bar')
        operator.hashes()
        print(operator.query)
        print(operator.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
