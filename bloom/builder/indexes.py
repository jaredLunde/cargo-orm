#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Builder Indexes`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from vital.debug import prepr

from bloom.fields import Field
from bloom.etc import types
from bloom.orm import QueryState
from bloom.expressions import *
from bloom.statements import *
from bloom.builder.utils import BaseCreator


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


class Index(BaseCreator):

    def __init__(self, orm, *fields, method=None, name=None, collate=None,
                 order=None, nulls=None, unique=None, concurrent=False,
                 fillfactor=None, fastupdate=None, buffering=False,
                 table=None, tablespace=None, partial=None):
        """`Create an Index`
            :see::func:bloom.builder.create_index
        """
        super().__init__(orm, name)
        self.fields = [field if isinstance(field, Field) else field._field
                       for field in fields]
        self._type = method
        self._table = table or self.orm.table
        if unique is not None:
            self._unique = unique
        elif isinstance(self.fields[0], Field):
            unique = False
            for field in self.fields:
                if field.unique:
                    unique = True
                if unique and not field.unique:
                    unique = False
                    break
            self._unique = unique
        else:
            self._unique = None

        self._partial = None
        if partial:
            self.partial(partial)

        self._tablespace = None
        if tablespace:
            self.tablespace(tablespace)

        self._collate = None
        if collate:
            self.collate(collate)

        self._op = None
        if order:
            self.order(order)

        self._nulls = None
        if nulls:
            self.nulls(nulls)

        self._concurrent = None
        if concurrent:
            self.concurrent()

        self._storage_parameters = []
        if fillfactor:
            self.fillfactor(fillfactor)

        if fastupdate is not None and self.type == 'gin':
            self.fastupdate(fastupdate)

        if buffering is not None and self.type == 'gist':
            self.buffering(buffering)

    def set_name(self, name):
        self._name = name

    def set_type(self, val):
        self._type = val
        return self

    @property
    def name(self):
        if self._name is None:
            idx_pref = self.fields[0]
            if isinstance(self.fields[0], Field):
                idx_pref = "_".join(field.field_name for field in self.fields)
            return safe("{}_{}_{}index".format(
                self.table,
                idx_pref,
                "unique_" if self._unique else ""))
        else:
            return self._cast_safe(self._name)

    @property
    def table(self):
        if self._table:
            return self._cast_safe(self._table)
        if isinstance(self.fields[0], Field):
            if self.fields[0].table:
                return safe(self.fields[0].table)
        raise ValueError('No table name was provided.')

    gin_types = {types.ARRAY, types.JSONB, types.JSONTYPE}

    @property
    def type(self):
        """ Defaults to 'btree' for most types, when a type has a choice
            between 'gin' and 'gist', 'gin' is chosen by default.
        """
        default = 'btree'
        if len(self.fields) == 1 and isinstance(self.fields[0], Field) and\
           self.fields[0].sqltype in self.gin_types:
                default = 'gin'
        return self._type or default

    @property
    def options(self):
        opt = []
        opt.append(safe(', '.join(field.field_name for field in self.fields)))
        if self._collate:
            opt.append(self._collate)
        if self._op:
            opt.append(self._op)
        if self._nulls:
            opt.append(self._nulls)
        return opt

    @property
    def method(self):
        cls = Clause(self.type, *self.options, join_with=" ", wrap=True)
        return Clause('USING', cls)

    @property
    def storage_parameters(self):
        if self._storage_parameters:
            return Clause('WITH',
                          *self._storage_parameters,
                          join_with=", ",
                          wrap=True)

    def concurrent(self):
        self._concurrent = Clause('CONCURRENTLY')
        return self

    def fastupdate(self, val=True):
        if val:
            type = 'ON'
        else:
            type = 'OFF'
        exp = safe('fastupdate').eq(safe(type))
        self._storage_parameters.append(exp)
        return self

    def buffering(self, val=True):
        if val:
            type = 'ON'
        else:
            type = 'OFF'
        exp = safe('buffering').eq(safe(type))
        self._storage_parameters.append(exp)
        return self

    def fillfactor(self, val):
        exp = safe('fillfactor').eq(type)
        self._storage_parameters.append(exp)
        return self

    def unique(self):
        self._unique = True
        return self

    def collate(self, val):
        self._collate = Clause('COLLATE', val)
        return self

    def asc(self):
        self._op = Clause('ASC')
        return self

    def desc(self):
        self._op = Clause('DESC')
        return self

    def order(self, op_class):
        if str(op_class).upper() == 'ASC':
            self.asc()
        elif str(op_class).upper() == 'DESC':
            self.desc()
        else:
            raise ValueError('Unrecognized order class `{}`'.format(op_class))
        return self

    def nulls(self, val):
        if str(val).upper() in {'FIRST', 'LAST'}:
            self._nulls = Clause('NULLS', safe(val))
        else:
            raise ValueError('Unrecognized value for NULLS `{}`'.format(vals))
        return self

    def tablespace(self, val):
        self._tablespace = Clause('TABLESPACE', self._cast_safe(val))
        return self

    def where(self, val):
        """ For partial indexes """
        self._partial = Clause('WHERE', self._cast_safe(val))
        return self

    partial = where

    @property
    def query(self):
        '''
        CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] [ name ] ON table
            [ USING method ]
            ( { column | ( expression ) } [ COLLATE collation ]
              [ opclass ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
            [ WITH ( storage_parameter = value [, ... ] ) ]
            [ TABLESPACE tablespace ]
            [ WHERE predicate ]
        '''
        self.orm.reset()
        type = 'INDEX'
        if self._unique:
            type = 'UNIQUE {}'.format(type)
        self._add(Clause('CREATE {}'.format(type),
                         self._concurrent or _empty,
                         self.name),
                  Clause('ON', self.table),
                  self.method,
                  self.storage_parameters,
                  self._tablespace,
                  self._partial)
        return Raw(self.orm)
