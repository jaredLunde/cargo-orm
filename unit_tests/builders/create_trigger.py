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

from cargo import ORM, db, fields, Function, Clause
from cargo.builder import create_trigger
from cargo.builder.triggers import Trigger


def new_field(type='text', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24, keyspace='aeioughrstlnmy')
    field.table = table or randkey(24, keyspace='aeioughrstlnmy')
    return field


class TestCreateTrigger(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        trigger = create_trigger(self.orm,
                                 'foo',
                                 'BEFORE',
                                 'INSERT',
                                 Clause('UPDATE OF', new_field()),
                                 table='foo',
                                 function=Function('log_something'),
                                 dry=True)
        print(trigger.query.mogrified)

        field = new_field('int', table='foo', name='bar')

        trigger = Trigger(self.orm, 'foo', 'BEFORE')
        trigger.table('foo')
        trigger.from_reference('bar')
        trigger.insert()
        trigger.condition(field > 10)
        trigger.update_of(field, new_field())
        trigger.immediately()
        trigger.to_function('log_something')
        print(trigger.query)
        print(trigger.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
