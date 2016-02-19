#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL Builder`
  ``Creates models from tables and tables from models``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import re

from vital.cache import cached_property, memoize
from vital.debug import prepr
from vital.tools import strings as string_tools

from bloom.exceptions import SchemaError
from bloom.cursors import CNamedTupleCursor
from bloom.orm import ORM, Model, QueryState
from bloom.expressions import *
from bloom.statements import *

from bloom.builder.fields import *
from bloom.builder.tables import *
from bloom.builder.indexes import *
from bloom.builder.foreign_keys import *
from bloom.builder.schemas import Schema
from bloom.builder.types import Type, EnumType
from bloom.builder.utils import *
from bloom.builder.views import View


__all__ = (
    'Modeller',
    'Builder',
    'create_models',
    'create_tables',
    'create_extension',
    'create_schema',
    'create_sequence',
    'create_function',
    'create_enum_type',
    'create_type',
    'create_view',
    'create_user'
)


_model_tpl = """
class {clsname}(Model):{comment}
    table = '{table}'
    {fields}
"""


#
# `Modelling data structures`
#


class BaseModeller(object):

    def __init__(self, orm):
        self.orm = orm
        self.orm.set_cursor_factory(CNamedTupleCursor)


class Modeller(BaseModeller):

    def __init__(self, orm, *tables, schema=None, banner=None):
        """`Model Builder`

            Generates models from Postgres tables.

            @*tables: (#str) one or several table names. If no table is
                given, the entire @schema will be searched.
            @schema: (#str) name of the table schema/search path to search
        """
        super().__init__(orm)
        self.tables = tables
        self.schema = schema or orm.schema or orm.client.schema or 'public'
        self.banner = banner

    @prepr('tables', 'schema')
    def __repr__(self): return

    @cached_property
    def tables_query(self):
        return _get_sql_file('get_tables')

    @cached_property
    def model_query(self):
        return _get_sql_file('get_model')

    @property
    def model_tpl(self):
        return _model_tpl

    @property
    def field_tpl(self):
        return _field_tpl

    def _get_schema_tables(self):
        return self.orm.execute(self.tables_query, {'schema': self.schema})\
            .fetchall()

    def _format_comment_cutoff(self, com, cutoff, separator=""):
        new_com = []
        length = 0
        for part in com.split(" "):
            length += len(part) + 1
            if length > cutoff:
                part = '\n    ' + separator + part
                length = len(part)
            new_com.append(part)
        return " ".join(new_com)

    def _format_inline_comment(self, comment):
        com = "#: " + comment
        cutoff = 67
        if len(com) > cutoff:
            com = self._format_comment_cutoff(com, cutoff, '#  ')
        return com.lstrip()

    def _format_block_comment(self, comment):
        com = '""" ' + comment + ' """'
        cutoff = 71
        if len(com) > cutoff:
            com = self._format_comment_cutoff(com, cutoff, '    ')
            com = com.rstrip('"') + '\n    """'
        return '\n    ' + com.lstrip()

    def create_field(self, field):
        args = field.args_string
        if field.foreign:
            field.cls = 'ForeignKey'
            args = "'{}.{}', {}".format(
                string_tools.underscore_to_camel(field.foreign.ref_table),
                field.foreign.ref_field,
                args
            ).strip(', ')
        comment = ""
        if field.field.comment:
            comment = self._format_inline_comment(field.field.comment)
        return self.field_tpl.format(
            field_comment=comment,
            field_name=field.field_name,
            field_cls=field.cls,
            field_args=args
        ).strip()

    def create_model(self, table, fields):
        clsname = string_tools.underscore_to_camel(table.name)
        fields = (self.create_field(FieldMeta(self.orm, field, table))
                  for field in fields)
        comment = ""
        if table.comment:
            comment = self._format_block_comment(table.comment)
        model = self.model_tpl.format(
            clsname=clsname,
            table=table.name,
            fields="\n    ".join(fields),
            comment=comment
        )
        return model

    def from_tables(self, *tables, write_mode=None):
        models = []
        for table in tables:
            q = self.orm.execute(self.model_query,
                                 {'schema': self.schema, 'table': table.table})
            r = q.fetchall()
            meta = TableMeta(self.orm, name=table.table,
                             schema=self.schema or r[0].schema,
                             comment=table.comment)
            models.append(self.create_model(meta, r))
        models = "\n".join(models)
        if write_mode and 'a' in write_mode:
            banner = "{}\n{}"
        else:
            banner = "#!/usr/bin/python" +\
                     "{}\nfrom bloom import Model, ForeignKey\n" +\
                     "from bloom.fields import *\n\n" +\
                     "{}"
        return (banner).format(self.banner or "", models)

    def from_schema(self, schema, write_mode=None):
        return self.from_tables(*self._get_schema_tables(),
                                write_mode=write_mode)

    def to_file(self, results, filename, write_mode='w'):
        with open(filename, write_mode) as f:
            f.write(results)

    def run(self, output_to=str, write_mode='w'):
        if self.tables:
            tables = []
            schema_tables = self._get_schema_tables()
            _tables = {table.table for table in self._get_schema_tables()}
            for table in self.tables:
                if table in _tables:
                    for t in schema_tables:
                        if t.table == table:
                            break
                    tables.append(t)
                else:
                    raise SchemaError('Table `{}` not found in schema `{}`'.
                                      format(table, self.schema))
            results = self.from_tables(*tables, write_mode=write_mode)
        else:
            results = self.from_schema(self.schema, write_mode=write_mode)
        if output_to is str:
            return results
        else:
            self.to_file(results, output_to, write_mode=write_mode)


class Builder(BaseModeller):

    def __init__(self, orm, schema=None, *models):
        """ `Table Builder`

             Generates the tables for given models.
        """
        self.orm = orm
        self.schema = schema or orm.schema or orm.client.schema or 'public'

    def run(self):
        query = create_schema(self.orm, self.schema)
        print(query)
        return query


def create_models(orm, *tables, banner=None, schema='public',
                  output_to=str, **kwargs):
    modeller = Modeller(orm, *tables, banner=banner, schema=schema)
    return modeller.run(output_to=output_to, **kwargs)


def create_tables(model):
    pass


def _cast_return(q, run=False):
    if not run:
        return q
    return q.execute()


def create_extension(orm, name, schema):
    """ -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    '''
    CREATE EXTENSION [ IF NOT EXISTS ] extension_name
        [ WITH ] [ SCHEMA schema_name ]
                 [ VERSION version ]
                 [ FROM old_version ]
    '''
    # http://www.postgresql.org/docs/9.1/static/sql-createextension.html
    # CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;
    pass


def create_schema(orm, name, authorization=None, not_exists=True, run=True):
    """ @name: (#str) name of the schema
        @authorization: (#str) username to create a schema for
        @not_exists: (#bool) adds |IF NOT EXISTS| clause to the statement
        @run: (#bool) |True| to execute the query before returning

        -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    schema = Schema(orm, name, authorization, not_exists)
    return _cast_return(schema.query, run)


def create_index(orm, field, method='btree', name=None, collate=None,
                 order=None, nulls=None, unique=False, concurrent=False):
    """ -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    '''
    CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] [ name ] ON table [ USING method ]
        ( { column | ( expression ) } [ COLLATE collation ]
          [ opclass ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
        [ WITH ( storage_parameter = value [, ... ] ) ]
        [ TABLESPACE tablespace ]
        [ WHERE predicate ]
    '''


def create_sequence(orm, name):
    """ -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    '''
    CREATE [ TEMPORARY | TEMP ] SEQUENCE name [ INCREMENT [ BY ] increment ]
        [ MINVALUE minvalue | NO MINVALUE ] [ MAXVALUE maxvalue | NO MAXVALUE ]
        [ START [ WITH ] start ] [ CACHE cache ] [ [ NO ] CYCLE ]
        [ OWNED BY { table.column | NONE } ]
    '''
    # http://www.postgresql.org/docs/9.1/static/sql-createsequence.html


def create_function(orm, file):
    """ -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    '''
    CREATE [ OR REPLACE ] FUNCTION
        name ( [ [ argmode ] [ argname ] argtype
               [ { DEFAULT | = } default_expr ] [, ...] ] )
        [ RETURNS rettype
          | RETURNS TABLE ( column_name column_type [, ...] ) ]
      { LANGUAGE lang_name
        | WINDOW
        | IMMUTABLE | STABLE | VOLATILE
        | CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
        | [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER
        | COST execution_cost
        | ROWS result_rows
        | SET configuration_parameter { TO value | = value | FROM CURRENT }
        | AS 'definition'
        | AS 'obj_file', 'link_symbol'
      } ...
        [ WITH ( attribute [, ...] ) ]

       create uid type -> schema and epoch required
    '''
    # http://www.postgresql.org/docs/9.1/static/sql-createfunction.html


def create_type(orm, name, *opt):
    """ -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    '''
    CREATE TYPE name (
        INPUT = input_function,
        OUTPUT = output_function
        [ , RECEIVE = receive_function ]
        [ , SEND = send_function ]
        [ , TYPMOD_IN = type_modifier_input_function ]
        [ , TYPMOD_OUT = type_modifier_output_function ]
        [ , ANALYZE = analyze_function ]
        [ , INTERNALLENGTH = { internallength | VARIABLE } ]
        [ , PASSEDBYVALUE ]
        [ , ALIGNMENT = alignment ]
        [ , STORAGE = storage ]
        [ , LIKE = like_type ]
        [ , CATEGORY = category ]
        [ , PREFERRED = preferred ]
        [ , DEFAULT = default ]
        [ , ELEMENT = element ]
        [ , DELIMITER = delimiter ]
    )

    CATEGORIES
    --------------------------
    A	Array types
    B	Boolean types
    C	Composite types
    D	Date/time types
    E	Enum types
    G	Geometric types
    I	Network address types
    N	Numeric types
    P	Pseudo-types
    S	String types
    T	Timespan types
    U	User-defined types
    V	Bit-string types
    X	unknown type
    '''


def create_enum_type(orm, name, *types, run=True):
    """ @orm: (:class:ORM)
        @name: (#str) the name of the type
        @types: (#str) types to create
        @run: (#bool) |True| to execute the query before returning

        -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    enum = EnumType(orm, name, *types)
    return _cast_return(enum.query, run)


def create_user(orm, name, in_role=None, in_group=None, role=None,
                admin=None, user=None, **options):
    """ -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    '''
    CREATE USER name [ [ WITH ] option [ ... ] ]

    where @options can be:

          SUPERUSER | NOSUPERUSER
        | CREATEDB | NOCREATEDB
        | CREATEROLE | NOCREATEROLE
        | CREATEUSER | NOCREATEUSER
        | INHERIT | NOINHERIT
        | LOGIN | NOLOGIN
        | REPLICATION | NOREPLICATION
        | CONNECTION LIMIT connlimit
        | [ ENCRYPTED | UNENCRYPTED ] PASSWORD 'password'
        | VALID UNTIL 'timestamp'
        | IN ROLE role_name [, ...]
        | IN GROUP role_name [, ...]
        | ROLE role_name [, ...]
        | ADMIN role_name [, ...]
        | USER role_name [, ...]
        | SYSID uid
    '''


def create_view(orm, name, as_, *columns, security_barrier=False,
                temporary=False, materialized=False, run=True):
    """ @orm: (:class:ORM)
        @name: (#str) name of the view
        @as_: (:class:Select) query to create a view for
        @columns: (#str|:class:Field) optional names to be used for columns
            of the view. If not given, the column names are deduced from
            the query.
        @security_barrier: (#bool) True to enable WITH (security_barrier)
        @temporary: (#bool) True to create a temporary view
        @materialized: (#bool) True to create materialized view
        @run: (#bool) |True| to execute the query before returning

        -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    view = View(orm, name, as_)
    view.columns(*columns)
    if security_barrier:
        view.security_barrier()
    if temporary:
        view.temporary()
    if materialized:
        view.materialized()
    return _cast_return(view.query, run)


if __name__ == '__main__':
    # TODO: CLI
    pass
