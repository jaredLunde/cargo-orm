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
from cargo.builder import create_domain
from cargo.builder.domains import Domain


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateDomain(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        domain = create_domain(self.orm, 'foo', 'int8', dry=True)
        print(domain.query.mogrified)
        domain = Domain(self.orm, 'foo', 'int8')
        domain.collate('LATIN1')
        domain.constraint('foobar')
        domain.not_null()
        print(domain.query)
        print(domain.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
