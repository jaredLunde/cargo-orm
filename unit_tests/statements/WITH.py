#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.statements.With`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import random
import unittest
from copy import copy
from kola import config

from vital.security import randkey

from bloom import *
from bloom.orm import QueryState
from bloom.statements import With


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_pool()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


def new_expression(cast=int):
    if cast == bytes:
        cast = lambda x: psycopg2.Binary(str(x).encode())
    return Expression(new_field(), '=', cast(12345))


def new_function(cast=int, alias=None):
    if cast == bytes:
        def cast(x):
            psycopg2.Binary(str(x).encode())
    return Function('some_func', cast(12345), alias=alias)


def new_clause(name='FROM', *vals):
    vals = vals or ['foobar']
    return Clause(name, *vals)


class TestWith(unittest.TestCase):
    orm = ORM()
    fields = [
        new_field('text', 'bar', name='textfield', table='foo'),
        new_field('int', 1234, name='uid', table='foo')]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_with_union(self):
        t = safe('t')
        n = safe('n')
        with (
          RAW(ORM().values(1), alias=t, recursive=(n,)) +
          SELECT(ORM().use(t), n+1)
        ) as orm:
            orm.use(t).limit(10).select(n)
        self.assertEqual(len(orm.result), 10)

        q = With(
            self.orm,
            Raw(ORM().values(1), alias=t, recursive=(n,)) +
            Select(ORM().use(t), n+1))
        q.orm.use(t).limit(10).select(n)
        self.assertEqual(len(q.execute().fetchall()), 10)


if __name__ == '__main__':
    # Unit test
    unittest.main()
