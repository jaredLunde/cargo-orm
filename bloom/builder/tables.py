#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Builder Tables`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from vital.cache import cached_property
from vital.debug import prepr

from bloom.builder.foreign_keys import ForeignKeyMeta
from bloom.builder.indexes import IndexMeta
from bloom.builder.utils import *


__all__ = ('TableMeta',)


class TableMeta(object):

    def __init__(self, orm, name, comment=None, schema='public'):
        self.orm = orm
        self.name = name
        self.schema = schema
        self.comment = comment

    @prepr('name', 'schema')
    def __repr__(self): return

    @property
    def index_query(self):
        return _get_sql_file('get_indexes')

    @property
    def foreign_key_query(self):
        return _get_sql_file('get_foreign_keys')

    def get_indexes(self):
        q = self.orm.execute(self.index_query,
                             {'table': self.name, 'schema': self.schema})
        return tuple(IndexMeta(name=index.index_name,
                               fields=index.fields,
                               unique=index.arg_unique,
                               primary=index.arg_primary,
                               type=index.type,
                               schema=index.schema,
                               table=index.table)
                     for index in q.fetchall())

    def get_foreign_keys(self):
        q = self.orm.execute(self.foreign_key_query,
                             {'table': self.name, 'schema': self.schema})
        return tuple(ForeignKeyMeta(table=key.table,
                                    field_name=key.field,
                                    ref_table=key.reference_table,
                                    ref_field=key.reference_field)
                     for key in q.fetchall())

    @cached_property
    def indexes(self):
        return self.get_indexes()

    @cached_property
    def primary_keys(self):
        for index in self.indexes:
            if index.primary:
                return index

    @cached_property
    def unique_indexes(self):
        return tuple(index for index in self.indexes if index.unique)

    @cached_property
    def plain_indexes(self):
        return tuple(index
                     for index in self.indexes
                     if not index.unique and not index.primary)

    @cached_property
    def foreign_keys(self):
        return self.get_foreign_keys()
