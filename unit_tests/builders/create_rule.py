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

from bloom import ORM, db, create_kola_db, fields, safe
from bloom.builder import create_rule
from bloom.builder.rules import Rule


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestCreateRule(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        rule = create_rule(self.orm,
                           'dont_update',
                           'UPDATE',
                           'foo',
                           self.orm.dry().select(1),
                           dry=True)
        print(rule.query.mogrified)
        rule = Rule(self.orm,
                    'dont_update',
                    'UPDATE',
                    'foo',
                    self.orm.dry().select(1))
        rule.replace()
        rule.instead()
        rule.condition(safe('foo.bar').gt(5))
        print(rule.query)
        print(rule.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
