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

import bloom.etc.translator.postgres

from bloom.fields.field import Field
from bloom.expressions import *
from bloom.relationships import Reference
from bloom.statements import *
from bloom.etc import types

from bloom.builder.foreign_keys import ForeignKeyMeta
from bloom.builder.indexes import IndexMeta
from bloom.builder.utils import *


__all__ = ('TableMeta', 'Table', 'Column')


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


class Table(BaseCreator):

    def __init__(self, orm, name, *column, local=False, temporary=False,
                 unlogged=False, not_exists=True, storage_parameters=None,
                 on_commit=None, inherits=None, tablespace=None,
                 type_name=None, like=None, constraints=None, **columns):
        """`Create a Table`
            :see::func:bloom.builder.create_table
        """
        super().__init__(orm, name)
        self._like = None
        self._global = False

        self._local = None
        if local:
            self.local()

        self._temporary = None
        if temporary:
            self.temporary()

        self._unlogged = None
        if unlogged:
            self.unlogged()

        self._not_exists = None
        if not_exists:
            self.not_exists()

        self._inherits = None
        if inherits:
            inherits = [inherits] if not isinstance(inherits, (tuple, list))\
                else inherits
            self.inherits(*inherits)

        self._on_commit = None
        if on_commit:
            self.on_commit(on_commit)

        self._storage_params = None
        if storage_parameters:
            self.storage_params(on_commit)

        self._tablespace = None
        if tablespace:
            self.tablespace(tablespace)

        self._type = None
        if type_name:
            self.type(type_name)

        self._columns = []
        if column:
            self.from_fields(*columns)

        if columns:
            self.set_columns(**columns)

        self._like = None
        if like:
            self.like(like)

        self._constraints = []
        if constraints:
            self.constraints(**constraints)

    def local(self):
        self._local = True
        return self

    def temporary(self):
        self._temporary = True
        return self

    def unlogged(self):
        self._unlogged = True
        return self

    def not_exists(self):
        self._not_exists = True
        return self

    def is_global(self):
        self._global = True
        return self

    def inherits(self, *tables):
        tables = map(self._cast_safe, tables)
        self._inherits = Clause('INHERITS', *tables, join_with=", ")
        return self

    def storage_params(self, **params):
        opt_ = []
        for k, v in params.items():
            if v is True:
                cls = safe(k)
            else:
                cls = safe(k).eq(v)
            opt_.append(cls)
        self._storage_params = Clause("WITH", *opt_, join_with=", ", wrap=True)
        return self

    def on_commit(self, what):
        self._on_commit = Clause('ON COMMIT', self._cast_safe(what))
        return self

    def tablespace(self, name):
        self._tablespace = Clause('TABLESPACE', self._cast_safe(name))
        return self

    @property
    def table_type(self):
        txt = ""
        if self._global:
            txt += 'GLOBAL '
        elif self._local:
            txt += 'LOCAL '
        if self._temporary:
            txt += 'TEMPORARY '
        if self._unlogged:
            txt += 'UNLOGGED '
        txt += 'TABLE '
        if self._not_exists:
            txt += 'IF NOT EXISTS '
        return Clause(txt.strip())

    def set_columns(self, **cols):
        self._columns = []
        for col_name, opts in cols.items():
            opts = map(self._cast_safe, opts)
            if self._type is not None:
                opts = [Clause('WITH OPTIONS', *opts)]
            self._columns.append(Clause("", safe(col_name), *opts))
        return self

    def from_fields(self, *fields):
        self._columns = []
        for field in fields:
            if not isinstance(field, Column):
                field = Column(field)
            if self._type is not None:
                field.typed()
            self._columns.append(field.expression)
        return self

    def like(self, val):
        self._like = Clause('LIKE', self._cast_safe(val))
        return self

    def constraints(self, *constraint, **constraints):
        """ @*constraint: (#str|:class:BaseExpression) single argument
                constraint to add
            @**constraints: (#str|:class:BaseExpression) key=value constraint
                pairs |constraint_name=constraint_value|
            ..
            { CHECK ( expression ) [ NO INHERIT ] |
              UNIQUE ( column_name [, ... ] ) index_parameters |
              PRIMARY KEY ( column_name [, ... ] ) index_parameters |
              EXCLUDE [ USING index_method ] (
                exclude_element WITH operator [, ... ] )
                index_parameters [ WHERE ( predicate ) ] |
              FOREIGN KEY ( column_name [, ... ] )
                REFERENCES reftable [ ( refcolumn [, ... ] ) ]
                [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ]
                [ ON DELETE action ] [ ON UPDATE action ] }
            [ DEFERRABLE | NOT DEFERRABLE ]
            [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]
            ..
        """
        _wrap = {'check', 'exclude'}
        self._constraints = list(map(self._cast_safe, constraint))
        for name, val in constraints.items():
            name = name.replace('_', ' ')
            wrap = False
            if name.lower() in _wrap:
                wrap = True
            if val is not True:
                cls = Clause(name, self._cast_safe(val), wrap=wrap)
            else:
                cls = Clause(name)
            self._constraints.append(cls)

    def timing(self, timing):
        """ @timing: (#str|:class:bloom.BaseExpression)
            * |[ DEFERRABLE | NOT DEFERRABLE ]|
            * |[ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]|
        """
        cls = Clause(timing)
        self._constraints.append(cls)
        return self

    def check(self, expression):
        """ @expression: (#str|:class:bloom.BaseExpression) """
        cls = Clause('CHECK', self._cast_safe(expression), wrap=True)
        self._constraints.append(cls)
        return self

    def _cast_fields(self, fields, clause_name=""):
        fields = fields if isinstance(fields, (tuple, list)) else [fields]
        fields = map(self._cast_safe, fields)
        return Clause("", *fields, join_with=", ", wrap=True)

    def primary_key(self, fields, *params):
        """ @fields: (#tuple|:class:Field|#str) one or more fields
            @params: (#str|:class:Clause) index parameters
        """
        fields = self._cast_fields(fields)
        cls = Clause('PRIMARY KEY', fields, *params, use_field_name=True)
        self._constraints.append(cls)
        return self

    def unique(self, fields, *params):
        """ @fields: (#tuple|:class:Field|#str) one or more fields
            @params: (#str|:class:Clause) index parameters
        """
        fields = self._cast_fields(fields)
        cls = Clause('UNIQUE', fields, *params, use_field_name=True)
        self._constraints.append(cls)
        return self

    def foreign_key(self, fields, ref_table, ref_fields, *params,
                    on_delete=None, on_update=None):
        """ @fields: (#tuple|:class:Field|#str) one or more fields
            @ref_table: (#str|:class:BaseExpression) referenced table
            @ref_fields: (#tuple|:class:Field|#str) one or more fields from
                the referenced table
            @params: (#str|:class:Clause) foreign key constraints
            @on_delete: (#str|:class:BaseExpression) one of
                * NO ACTION
                * RESTRICT
                * CASCADE
                * SET NULL
                * SET DEFAULT
            @on_update: (#str|:class:BaseExpression) one of
                * NO ACTION
                * RESTRICT
                * CASCADE
                * SET NULL
                * SET DEFAULT
        """
        params = list(params)
        fields = self._cast_fields(fields)
        ref_fields = self._cast_fields(ref_fields)
        ref_table = Clause('references', self._cast_safe(ref_table),
                           ref_fields)
        if on_delete:
            on_delete = Clause('ON DELETE', self._cast_safe(on_delete))
            params.append(on_delete)
        if on_update:
            on_update = Clause('ON UPDATE', self._cast_safe(on_update))
            params.append(on_update)
        opts = [fields, ref_table]
        opts.extend(params)
        cls = Clause('FOREIGN KEY',
                     *opts,
                     use_field_name=True)
        self._constraints.append(cls)
        return self

    @property
    def query(self):
        ''' ..
            CREATE [ [ GLOBAL | LOCAL ] { TEMPORARY | TEMP } | UNLOGGED ]
                TABLE [ IF NOT EXISTS ] table_name ( [
              { column_name data_type [ COLLATE collation ]
                [ column_constraint [ ... ] ]
                | table_constraint
                | LIKE source_table [ like_option ... ]
              } [, ... ]
            ] )
            [ INHERITS ( parent_table [, ... ] ) ]
            [ WITH ( storage_parameter [= value] [, ... ] )
                | WITH OIDS | WITHOUT OIDS ]
            [ ON COMMIT { PRESERVE ROWS | DELETE ROWS | DROP } ]
            [ TABLESPACE tablespace_name ]


            --------------------------------------------------------------------
            where table_constraint is:

            [ CONSTRAINT constraint_name ]
            { CHECK ( expression ) [ NO INHERIT ] |
              UNIQUE ( column_name [, ... ] ) index_parameters |
              PRIMARY KEY ( column_name [, ... ] ) index_parameters |
              EXCLUDE [ USING index_method ] (
                exclude_element WITH operator [, ... ] )
                index_parameters [ WHERE ( predicate ) ] |
              FOREIGN KEY ( column_name [, ... ] )
                REFERENCES reftable [ ( refcolumn [, ... ] ) ]
                [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ]
                [ ON DELETE action ]
                [ ON UPDATE action ] }
            [ DEFERRABLE | NOT DEFERRABLE ]
            [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]


            --------------------------------------------------------------------
            and like_option is:

            { INCLUDING | EXCLUDING }
            { DEFAULTS | CONSTRAINTS | INDEXES | STORAGE | COMMENTS | ALL }


            --------------------------------------------------------------------
            exclude_element in an EXCLUDE constraint is:

            { column_name | ( expression ) }
            [ opclass ]  [ ASC | DESC ]
            [ NULLS { FIRST | LAST } ]
            ..
        '''
        self.orm.reset()
        cols = None
        if self._columns or self._constraints:
            cols = self._columns + self._constraints
            cols = Clause("", *cols,  join_with=', ', wrap=True,
                          use_field_name=True)
        self._add(Clause('CREATE', self.table_type, self.name),
                  cols,
                  self._like,
                  self._inherits,
                  self._storage_params,
                  self._on_commit,
                  self._tablespace)
        return Raw(self.orm)


class Column(BaseCreator):

    def __init__(self, field, check=None, not_null=None, unique=None,
                 primary=None, default=None, references=None, timing=None,
                 translator=bloom.etc.translator.postgres, parameters=None,
                 data_type=None, typed=False):
        """ @field: (:class:Field)
            @check: (#str|:class:BaseExpression) check constraint is the most
                generic constraint type. It allows you to specify that the
                value in a certain column must satisfy a Boolean (truth-value)
                expression
            @not_null: (#bool) |True| if the column cannot be null
            @unique: (#bool|#str|:class:Clause) |True| to set plain
                |UNIQUE| constraint #str or :class:Clause to set
                parameterized constraint
            @primary: (#bool|#str|:class:Clause) |True| to set plain
                |PRIMARY KEY| constraint #str or :class:Clause to set
                parameterized constraint
            @default: (#str|#int|#bool|#list|:class:BaseExpression)
                sets the default value constraint
            @references: (#str|:class:BaseExpression) sets the |REFERENCES|
                clause to the constraints provided
            @timing: (#str|:class:Clause) one of:
                * |DEFERRABLE|
                * |NOT DEFERRABLE|
                * |INITIALLY IMMEDIATE|
                * |INITIALLY DEFERRED|
            @translator: field type translator for converting bloom
                :class:Field objects to sql types
            @parameters: (#str|:class:BaseExpression) column data type
                parameters
            @data_type: (#str|:class:BaseExpression) column data type
            @typed: (#bool) |True| if this is for a typed table
        """
        self._name = field.field_name
        self._clauses = []
        self._field = field
        self._not_null = not_null if not_null is not None else field.notNull
        self._parameters = parameters
        self._data_type = data_type
        self._translator = translator
        self._typed = None
        if typed:
            self.typed()

        self._default = None
        default = default if default is not None else field.default
        if default is not None:
            self.default(default)

        self._check = None
        if check:
            self.check(check)

        self._unique = None
        unique = unique if unique is not None else field.unique
        if unique:
            unique = [unique] if not isinstance(unique, (tuple, list)) else \
                unique
            self.unique(*unique)

        self._primary = None
        primary = primary if primary is not None else field.primary
        if primary:
            primary = [primary] if not isinstance(primary, (tuple, list)) else\
                primary
            self.primary(*primary)

        self._references = None
        if references is not None:
            references = references
        elif hasattr(field, 'ref'):
            references = field.ref
        if references:
            self.references(references)

        self._timing = None
        if timing:
            self.timing(timing)

    def set_type(self, data_type):
        self._data_type = safe(data_type)

    def typed(self):
        self._typed = True
        return self

    def not_null(self):
        self._not_null = True
        return self

    def default(self, val=None):
        self._default = Clause('DEFAULT', val)
        return self

    def check(self, expression):
        """ @expression: (#str|:class:bloom.BaseExpression) """
        self._check = Clause('CHECK', self._cast_safe(expression), wrap=True)
        return self

    def unique(self, *params):
        if len(params) == 1 and params[0] in {True, False}:
            params = [_empty]
        else:
            params = map(self._cast_safe, params)
        self._unique = Clause('UNIQUE', *params)
        return self

    def primary(self, *params):
        if len(params) == 1 and params[0] in {True, False, None}:
            params = [_empty]
        else:
            params = map(self._cast_safe, params)
        self._primary = Clause('PRIMARY KEY', *params)
        return self

    def timing(self, when):
        self._timing = Clause(str(when))
        return self

    def _get_col_ref(self):
        field = self._field
        return Clause('REFERENCES',
                      Function(field.ref.field.table,
                               safe(field.ref.field.field_name)),
                      *field.ref.constraints)

    def references(self, val):
        if not isinstance(val, Reference):
            self._references = Clause('REFERENCES', self._cast_safe(val))
        else:
            self._references = self._get_col_ref()
        return self

    def _add(self, *clauses):
        self._clauses = []
        self._clauses.extend(filter(lambda x: x is not None, clauses))

    @property
    def field_type(self):
        """ -> :class:safe """
        if self._data_type:
            return self._data_type
        opt = None
        sqltype = self._field.sqltype
        lentypes = {types.CHAR, types.VARCHAR, types.USERNAME, types.EMAIL}
        if self._parameters:
            opt = self._parameters
        elif sqltype in lentypes and self._field.maxlen and \
          self._field.maxlen > 0:
            opt = self._field.maxlen
        elif sqltype in {types.NUMERIC, types.DECIMAL} and \
          self._field.precision and self._field.precision > 0:
            opt = self._field.precision
        return safe(self._translator.translate_to(self._field.sqltype, opt))

    @property
    def expression(self):
        ''' -> :class:Clause
        ..
        { NOT NULL |
          NULL |
          CHECK ( expression ) [ NO INHERIT ] |
          DEFAULT default_expr |
          UNIQUE index_parameters |
          PRIMARY KEY index_parameters |
          REFERENCES reftable [ ( refcolumn ) ]
            [ MATCH FULL | MATCH PARTIAL | MATCH SIMPLE ]
            [ ON DELETE action ] [ ON UPDATE action ] }
        [ DEFERRABLE | NOT DEFERRABLE ]
        [ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]


        ------------------------------------------------------------------------
        index_parameters in UNIQUE, PRIMARY KEY, and EXCLUDE constraints are:

        [ WITH ( storage_parameter [= value] [, ... ] ) ]
        [ USING INDEX TABLESPACE tablespace_name ]


        ------------------------------------------------------------------------
        exclude_element in an EXCLUDE constraint is:

        { column_name | ( expression ) }
        [ opclass ]  [ ASC | DESC ]
        [ NULLS { FIRST | LAST } ]
        ..
        '''
        opt = (
            self.field_type,
            self._not_null,
            self._check,
            self._default,
            self._unique,
            self._primary,
            self._references,
            self._timing
        )
        if self._typed:
            opt = filter(lambda x: x is not None, opt)
            opt = [Clause('WITH OPTIONS', *opt)]
        self._add(self.name, *opt)
        return Clause("", *self._clauses)

    # For consistency only
    query = expression
