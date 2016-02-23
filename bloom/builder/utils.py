#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Builder Utils`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import os

from docr import Docr

from bloom import fields
from bloom.expressions import safe
from bloom.etc.translator import postgres


__all__ = ('_get_sql_file', '_get_docr', '_find_sql_field', 'BaseCreator')


path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _find_sql_field(datatype, udtype, category):
    """ See http://www.postgresql.org/docs/9.5/static/datatype.html """
    return postgres.translate_from(datatype, udtype, category)


def _get_sql_file(name, searchpath=''):
    f = None
    searchpath = os.path.join(path, 'etc/_postgres_assets', searchpath,
                              name + '.sql')
    sql_file = None
    with open(searchpath, 'r') as f:
        sql_file = f.read()
    return sql_file


def _get_docr(field):
    return Docr(getattr(fields, field))


class BaseCreator(object):

    def __init__(self, orm, name):
        self.orm = orm.copy()
        if name is not None:
            self._name = safe(name)
        else:
            self._name = None

    def _add(self, *clauses):
        for clause in clauses:
            if clause:
                self.orm.state.add(clause)

    def _cast_safe(self, val):
        if isinstance(val, str):
            return safe(val)
        else:
            return val

    @property
    def name(self):
        return self._cast_safe(self._name)

    @property
    def string(self):
        return self.query.string

    @property
    def params(self):
        return self.query.params

    def execute(self):
        return self.query.execute()

    create = execute
