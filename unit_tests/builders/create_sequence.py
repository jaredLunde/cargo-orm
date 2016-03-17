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

from cargo import ORM, db, fields
from cargo.builder import create_sequence


def new_field(type='text', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateSequence(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        print(create_sequence(self.orm, 'foo', owned_by='foo.bar', dry=True))
        field = new_field(table='foo', name='bar')
        print(create_sequence(self.orm, 'foo', owned_by=field, dry=True))


if __name__ == '__main__':
    # Unit test
    unittest.main()
