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

from cargo import ORM, db, fields, safe, Function
from cargo.builder import create_rule
from cargo.builder.extras import UIDFunction


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
            funcs.append(Function('foo_%s.cargo_uid' % x, alias='uid%s' % x))
        print(orm.select(*funcs))



if __name__ == '__main__':
    # Unit test
    unittest.main()
