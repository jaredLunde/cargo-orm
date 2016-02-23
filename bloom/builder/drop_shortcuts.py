#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Bloom SQL shortcut functions for dropping tables, models, role, etc`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
from bloom.builder.roles import Role, User
from bloom.builder.views import View
from bloom.builder.indexes import *
from bloom.builder.foreign_keys import *
from bloom.builder.schemas import Schema
from bloom.builder.sequences import *
from bloom.builder.utils import *


__all__ = (
    'drop_cast',  #: 10 MIN
    'drop_database',  #: 10 MIN
    'drop_domain',  #: 10 MIN
    'drop_operator',  #: 10 MIN
    'drop_extension',  #: 10 MIN
    'drop_schema',  #: 10 MIN
    'drop_index',  #: 10 MIN
    'drop_sequence',  #: 10 MIN
    'drop_function',  #: 10 MIN
    'drop_tablespace',  #: 10 MIN
    'drop_trigger',  #: 10 MIN
    'drop_type',  #: 10 MIN
    'drop_enum_type',  #: 10 MIN
    'drop_role',  #: 10 MIN
    'drop_rule',  #: 10 MIN
    'drop_user',  #: 10 MIN
    'drop_view'  #: 10 MIN
)


# NOTE: EST 3.5-5 HOURS


def _cast_return(q, run=False):
    if not run:
        return q
    return q.execute()


def drop_extension(orm, name, schema):
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


def drop_database(orm, name, schema):
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


def drop_schema(orm, name, authorization=None, not_exists=True, run=True):
    """ @name: (#str) name of the schema
        @authorization: (#str) username to create a schema for
        @not_exists: (#bool) adds |IF NOT EXISTS| clause to the statement
        @run: (#bool) |True| to execute the query before returning

        -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    schema = Schema(orm, name, authorization, not_exists)
    return _cast_return(schema.query, run)


def drop_index(orm, field, method='btree', name=None, collate=None,
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


def drop_sequence(orm, name):
    """ -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """



def drop_function(orm, file):
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


def drop_type(orm, name, *opt):
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


def drop_enum_type(orm, name, *types, run=True):
    """ @orm: (:class:ORM)
        @name: (#str) the name of the type
        @types: (#str) types to create
        @run: (#bool) |True| to execute the query before returning

        -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    enum = EnumType(orm, name, *types)
    return _cast_return(enum.query, run)


def drop_role(orm, name, *options, in_role=None, in_group=None, role=None,
                admin=None, user=None, password=None, password_encryption=True,
                valid_until=None, connlimit=None, run=True):
    """ @orm: (:class:ORM)
        @name: (#str) name of the role
        @*options: (#str) names of role options
            * SUPERUSER / NOSUPERUSER
            * CREATEDB / NOCREATEDB
            * CREATEROLE / NOCREATEROLE
            * CREATEUSER / NOCREATEUSER
            * INHERIT / NOINHERIT
            * LOGIN / NOLOGIN
            * REPLICATION / NOREPLICATION
        @in_role: (#str|#tuple) sets the |IN ROLE| option
        @in_group: (#str|#tuple) sets the |IN GROUP| option
        @role: (#str|#tuple) sets the |ROLE| option
        @admin: (#str|#tuple) sets the |ADMIN| option
        @user: (#str|#tuple) sets the |USER| option
        @password: (#str) sets a password for the role
        @password_encryption: (#bool) encrypts @password
        @valid_until: (:class:datetime.datetime) sets the |VALID UNTIL| option
        @connlimit: (#int) sets the |CONNECTION LIMIT| option
        @run: (#bool) True to execute the query right away

        -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    role = Role(orm, name, *options, in_role=in_role, in_group=in_group,
                role=role, admin=admin, user=user, password=password,
                password_encryption=password_encryption,
                valid_until=valid_until, connlimit=connlimit)
    return _cast_return(role.query, run)


def drop_trigger(orm, name, *options, in_role=None, in_group=None, role=None,
                admin=None, user=None, password=None, password_encryption=True,
                valid_until=None, connlimit=None, run=True):
    """ @orm: (:class:ORM)
        @name: (#str) name of the role
        @*options: (#str) names of role options
            * SUPERUSER / NOSUPERUSER
            * CREATEDB / NOCREATEDB
            * CREATEROLE / NOCREATEROLE
            * CREATEUSER / NOCREATEUSER
            * INHERIT / NOINHERIT
            * LOGIN / NOLOGIN
            * REPLICATION / NOREPLICATION
        @in_role: (#str|#tuple) sets the |IN ROLE| option
        @in_group: (#str|#tuple) sets the |IN GROUP| option
        @role: (#str|#tuple) sets the |ROLE| option
        @admin: (#str|#tuple) sets the |ADMIN| option
        @user: (#str|#tuple) sets the |USER| option
        @password: (#str) sets a password for the role
        @password_encryption: (#bool) encrypts @password
        @valid_until: (:class:datetime.datetime) sets the |VALID UNTIL| option
        @connlimit: (#int) sets the |CONNECTION LIMIT| option
        @run: (#bool) True to execute the query right away

        -> :class:Raw if run is |False|, otherwise the client cursor is
            returned
    """
    role = Role(orm, name, *options, in_role=in_role, in_group=in_group,
                role=role, admin=admin, user=user, password=password,
                password_encryption=password_encryption,
                valid_until=valid_until, connlimit=connlimit)
    return _cast_return(role.query, run)

def drop_user(orm, name, *options, in_role=None, in_group=None, role=None,
                admin=None, user=None, password=None, password_encryption=True,
                valid_until=None, connlimit=None, run=True):
    """ :see::func:drop_role """
    user = User(orm, name, *options, in_role=in_role, in_group=in_group,
                role=role, admin=admin, user=user, password=password,
                password_encryption=password_encryption,
                valid_until=valid_until, connlimit=connlimit)
    return _cast_return(user.query, run)


def drop_view(orm, name, as_, *columns, security_barrier=False,
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


def drop_cast():
    pass


def drop_trigger():
    pass


def drop_domain():
    pass


def drop_language():
    pass


def drop_operator():
    pass


def drop_rule():
    pass


def drop_tablespace():
    pass
