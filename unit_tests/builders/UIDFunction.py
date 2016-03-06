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

from bloom import ORM, db, create_kola_db, fields, safe, Function
from bloom.builder import create_rule
from bloom.builder.extras import UIDFunction


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateUIDFunction(unittest.TestCase):

    def test_create(self):
        funcs = []
        for x in range(3):
            orm = ORM(schema='foo_%s' % x)
            uf = UIDFunction(orm)
            # print(uf.query)
            uf.execute()
            funcs.append(Function('foo_%s.bloom_uid' % x, alias='uid%s' % x))
        print(orm.select(*funcs))



if __name__ == '__main__':
    # Unit test
    unittest.main()
