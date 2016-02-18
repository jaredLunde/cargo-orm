#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Builder Indexes`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.orm import QueryState
from vital.debug import prepr


__all__ = ('IndexMeta', 'Index')


class IndexMeta(object):
    types = ('btree', 'gin', 'gist', 'spgist', 'hash')

    def __init__(self, name, fields, unique=False, primary=False,
                 type="btree", schema=None, table=None):
        self.name = name
        self.fields = fields if isinstance(fields, (tuple, list)) else\
            [fields]
        self.unique = unique
        self.primary = primary
        self.type = type
        self.schema = schema
        self.table = table

    @prepr('name', 'fields', 'table')
    def __repr__(self): return


class Index(object):

    def __init__(self, orm, name, table=None, type=None):
        self.orm = orm
        self.table = table or self.orm.table
        self._type = type
        self.name = name
        self.string = None
        self.state = QueryState()
        self.compile()

    @property
    def type(self):
        """ Defaults to 'btree' for most types, when a type has a choice
            between 'gin' and 'gist', 'gin' is chosen by default.
        """
        return self._type or 'btree'

    def unique(self):
        self.compile()
        return self

    def collate(self):
        self.compile()
        return self

    def tablespace(self):
        self.compile()
        return self

    def asc(self):
        self.compile()
        return self

    def desc(self):
        self.compile()
        return self

    def nulls(self):
        self.compile()
        return self

    def with_(self):
        self.compile()
        return self

    def where(self):
        self.compile()
        return self

    def compile(self, concurrent=False):
        self.string = ""
        return self.string

    clauses = ('UNIQUE', 'CONCURRENTLY', 'ON', 'USING', 'COLLATE', 'WITH',
               'ASC', 'DESC', 'NULLS', 'TABLESPACE', 'WHERE')

    def create(self, concurrent=False):
        self.compile(concurrent=concurrent)
        clauses = []
        for clause in self.ordinal:
            cls = self.state.get(clause)
            if cls:
                clauses.append(clause)
