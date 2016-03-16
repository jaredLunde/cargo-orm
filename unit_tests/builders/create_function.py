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

from cargo import ORM, db, create_kola_db, fields, Clause, safe
from cargo.builder import create_function
from cargo.builder.functions import Function


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(function='char', value=None, name=None, table=None):
    field = getattr(fields, function.title())(value=value)
    field.field_name = name or randkey(24, keyspace='aeioughrstlnmy')
    field.table = table or randkey(24, keyspace='aeioughrstlnmy')
    return field


class TestCreateFunction(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        function = create_function(
            self.orm,
            'dup(in int, out f1 int, out f2 text)',
            "$$ SELECT $1, CAST($1 AS text) || ' is text' $$",
            'IMMUTABLE',
            'RETURNS NULL ON NULL INPUT',
            language='SQL',
            returns='text',
            replace=True,
            dry=True)
        print(function.query.mogrified)

        function = Function(self.orm, 'foo', '$$DECLARE; END;$$')
        function.options(security_definer=Clause(
                            'SET', safe('search_path = admin, pg_temp')),
                         element='float4')
        print(function.query)
        print(function.query.mogrified)

        function = Function(self.orm,
                            'foo()',
                            Clause('STABLE'),
                            cost=70,
                            as_=('obj_file', 'link_symbol'))
        print(function.query)
        print(function.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
