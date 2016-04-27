"""

  `Cargo SQL Builder Tables`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2016 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
from vital.cache import cached_property
from vital.debug import preprX

from cargo.etc.translator import postgres

from cargo.fields.field import Field
from cargo.fields.identifier import Serial, BigSerial, SmallSerial
from cargo.fields.encrypted import Encrypted
from cargo.expressions import *
from cargo.relationships import Reference
from cargo.statements import *
from cargo.etc import types

from cargo.builder.fields import *
from cargo.builder.foreign_keys import ForeignKeyMeta
from cargo.builder.indexes import IndexMeta
from cargo.builder.utils import *


__all__ = ('TableMeta', 'Table')


class TableMeta(object):

    def __init__(self, orm, name, comment=None, schema='public'):
        self.orm = orm
        self.name = name
        self.schema = schema
        self.comment = comment

    __repr__ = preprX('name', 'schema')

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
                 unlogged=False, not_exists=False, storage_parameters=None,
                 on_commit=None, inherits=None, tablespace=None,
                 type_name=None, like=None, constraints=None, **columns):
        """`Create a Table`
            :see::func:cargo.builder.create_table
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
            self.from_fields(*column)

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
        self._inherits = CommaClause('INHERITS', *tables)
        return self

    def storage_params(self, **params):
        opt_ = []
        for k, v in params.items():
            if v is True:
                cls = safe(k)
            else:
                cls = safe(k).eq(v)
            opt_.append(cls)
        self._storage_params = ValuesClause("WITH", *opt_)
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
        """ @*constraint: (#str or :class:BaseExpression) single argument
                constraint to add
            @**constraints: (#str or :class:BaseExpression) key=value
                constraint pairs |constraint_name=constraint_value|
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
        for type, val in constraints.items():
            type = type.replace('_', ' ')
            wrap = False
            if type.lower() in _wrap:
                wrap = True
            if val is not True:
                cls = Clause(type, self._cast_safe(val), wrap=wrap)
            else:
                cls = Clause(type)
            self._constraints.append(cls)

    def timing(self, timing):
        """ @timing: (#str or :class:cargo.BaseExpression)
            * |[ DEFERRABLE | NOT DEFERRABLE ]|
            * |[ INITIALLY DEFERRED | INITIALLY IMMEDIATE ]|
        """
        cls = Clause(timing)
        self._constraints.append(cls)
        return self

    def check(self, expression):
        """ @expression: (#str or :class:cargo.BaseExpression) """
        cls = Clause('CHECK', self._cast_safe(expression), wrap=True)
        self._constraints.append(cls)
        return self

    def _cast_fields(self, fields, clause_name=""):
        fields = fields if isinstance(fields, (tuple, list)) else [fields]
        fields = map(self._cast_safe, fields)
        return ValuesClause("", *fields)

    def primary_key(self, fields, *params):
        """ `Defines the primary key(s)`
            @fields: (#tuple or :class:Field or #str) one or more fields
            @params: (#str or :class:Clause) index parameters
        """
        fields = self._cast_fields(fields)
        cls = Clause('PRIMARY KEY', fields, *params, use_field_name=True)
        self._constraints.append(cls)
        return self

    def unique(self, fields, *params):
        """ `Creates a unique constraint`
            @fields: (#tuple or :class:Field or #str) one or more fields
            @params: (#str or :class:Clause) unique parameters
        """
        fields = self._cast_fields(fields)
        cls = Clause('UNIQUE', fields, *params, use_field_name=True)
        self._constraints.append(cls)
        return self

    def foreign_key(self, fields, ref_table, ref_fields, *params,
                    on_delete=None, on_update=None):
        """ `Defines foreign key field(s)`
            @fields: (#tuple or :class:Field or #str) one or more fields
            @ref_table: (#str or :class:BaseExpression) referenced table
            @ref_fields: (#tuple or :class:Field or #str) one or more fields
                from the referenced table
            @params: (#str or :class:Clause) foreign key constraints
            @on_delete: (#str or :class:BaseExpression) one of
                * NO ACTION
                * RESTRICT
                * CASCADE
                * SET NULL
                * SET DEFAULT
            @on_update: (#str or :class:BaseExpression) one of
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
            cols = ValuesClause("", *cols, use_field_name=True)
        self._add(Clause('CREATE', self.table_type, self.name),
                  cols,
                  self._like,
                  self._inherits,
                  self._storage_params,
                  self._on_commit,
                  self._tablespace)
        return Raw(self.orm)
