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
from cargo.builder import create_extension
from cargo.builder.extensions import Extension


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateExtension(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        extension = create_extension(self.orm, 'citext', dry=True)
        print(extension.query)
        field = new_field('int', table='foo', name='bar')
        extension = Extension(self.orm, 'citext')
        extension.old_version('3.0.0')
        extension.version('3.0.1')
        extension.schema('pandora')
        print(extension.query)


if __name__ == '__main__':
    # Unit test
    unittest.main()
