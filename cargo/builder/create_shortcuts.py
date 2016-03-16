"""

  `Cargo SQL shortcut functions for creating tables, models, role, etc`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import sys

from cargo.builder.casts import Cast
from cargo.builder.comments import Comment
from cargo.builder.databases import Database
from cargo.builder.domains import Domain
from cargo.builder.extensions import Extension
from cargo.builder.functions import Function
from cargo.builder.indexes import Index
from cargo.builder.operators import Operator
from cargo.builder.roles import Role, User
from cargo.builder.rules import Rule
from cargo.builder.schemas import Schema
from cargo.builder.sequences import Sequence
from cargo.builder.tables import Table
from cargo.builder.tablespaces import Tablespace
from cargo.builder.triggers import Trigger
from cargo.builder.types import Type, EnumType, RangeType
from cargo.builder.utils import *
from cargo.builder.views import View


__all__ = (
    'create_cast',
    'comment_on',
    'create_database',
    'create_domain',
    'create_operator',
    'create_extension',
    'create_schema',
    'create_index',
    'create_sequence',
    'create_function',
    'create_table',
    'create_type',
    'create_range_type',
    'create_enum_type',
    'create_role',
    'create_rule',
    'create_tablespace',
    'create_trigger',
    'create_user',
    'create_view'
)


def _cast_return(q, dry=False):
    if not dry:
        return q.execute()
    return q


def comment_on(*args, dry=False, **kwargs):
    """ `Create a Comment`
        @orm: (:class:ORM)
        @name: (#str) name of the table
        @type: (#str) |COLUMN|, |TABLE|, |SCHEMA|, etc...
        @identifier: (#str) if you're commenting on a cast for instance,
            the identifier is |(source_type AS target_type)|, and on a
            column |relation_name.column_name|.
            See http://www.postgresql.org/docs/9.5/static/sql-comment.html
        @comment: (#str) the comment content

    """
    comment = Comment(*args, **kwargs)
    return _cast_return(comment, dry)


def create_table(*args, dry=False, **kwargs):
    """ `Create a Table`
        @orm: (:class:ORM)
        @name: (#str) name of the table
        @*column: (:class:Field or :class:cargo.builders.tables.Column)
            columns to add to the table with all constraints defined
            within the object itself
        @local: (#bool) |True| sets the |LOCAL| flag in
            the |CREATE| clause
        @temporary: (#bool) |True| sets the |TEMPORARY| flag in
            the |CREATE| clause
        @unlogged: (#bool) |True| sets the |UNLOGGED| flag in
            the |CREATE| clause
        @not_exists: (#bool) |True| sets the |IF NOT EXISTS| flag in
            the |CREATE| clause
        @storage_parameters: (#dict) |param_name: value|
        @on_commit: (#str or :class:Clause) one of |PRESERVE ROWS|,
            |DELETE ROWS|, |DROP|
        @inherits: (#str or #tuple(#str)) name or names of the tables to
            inherit from
        @tablespace: (#str) name of the tablespace
        @type_name: (#str) creates a typed table, which takes its
            structure from the specified composite type
        @like: (#str or :class:BaseExpression) specifies a table from which
            the new table automatically copies all column names, their data
            types, and their not-null constraints
        @constraints: (#dict of {#str: #str or :class:BaseExpression})
            pairs of |constraint_name: constraint_value| to add to the
            table, e.g. |{check: (fielda > 10)}|
        @**columns: (#list or #tuple) of |column_name=((opt_name, opt_val),)|
            option #tuple pairs
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    table = Table(*args, **kwargs)
    return _cast_return(table, dry)


def create_extension(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the extension
        @schema: (#str) schema name
        @version: (#str) value for |VERSION| clause
        @old_version: (#str) value for |FROM| clause
        @not_exists: (#bool) True to include |IF NOT EXISTS| clause
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    ext = Extension(*args, **kwargs)
    return _cast_return(ext, dry)


def create_schema(orm, name, authorization=None, not_exists=True, dry=False):
    """ @orm: (:class:ORM)
        @name: (#str) name of the schema
        @authorization: (#str) username to create a schema for
        @not_exists: (#bool) adds |IF NOT EXISTS| clause to the statement
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    schema = Schema(orm, name, authorization, not_exists)
    return _cast_return(schema, dry)


def create_index(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @field: (#str or :class:Field) the field to create an index on
        @method: (#str) type of index to create
        @name: (#str) name of the index, one will be autogenerated if not
            given
        @table: (#str) table to create the index on
        @collate: (#str) collation for |COLLATE| clause
        @order: (#str or :class:Clause) |ASC| or |DESC|
        @nulls: (#str or :class:Clause) |FIRST| or |LAST
        @unique: (#bool) True to create a unique index
        @concurrent: (#bool) True to concurrently create the index
        @buffering: (#bool) False to set |BUFFERING OFF| for
            gist indexes
        @fastupdate: (#bool) True to implement |FASTUPDATE ON| for
            gin indexes
        @fillfactor: (#int [10-100]) fillfactor for an index is a percentage
            that determines how full the index method will try to pack
            index pages
        @tablespace: (#str) name of the tablespace to create the index in
        @partial: (#str or :class:BaseExpression) sets the |WHERE| clause
            for partial indexes
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    index = Index(*args, **kwargs)
    return _cast_return(index, dry)


def create_sequence(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the sequence
        @incr: (#int) specifies which value is added to the current
            sequence value to create a new value. A positive value will
            make an ascending sequence, a negative one a descending sequence.
            The default value is 1.
        @minval: (#int) The optional clause MINVALUE minvalue determines the
            minimum value a sequence can generate. If this clause is not
            supplied or NO MINVALUE is specified, then defaults will be used.
            The defaults are 1 and -263-1 for ascending and descending
            sequences, respectively.
        @start: (#int) allows the sequence to begin anywhere. The default
            starting value is minvalue for ascending sequences and maxvalue
            for descending ones.
        @cache: (#int) specifies how many sequence numbers are to be
            preallocated and stored in memory for faster access.
            The minimum value is 1 (only one value can be generated at a
            time, i.e., no cache), and this is also the default.
        @cycle: (#bool) The CYCLE option allows the sequence to wrap around
            when the maxvalue or minvalue has been reached by an ascending or
            descending sequence respectively. If the limit is reached, the
            next number generated will be the minvalue or maxvalue,
            respectively.
        @owned_by: (#str or :class:Field) The OWNED BY option causes the
            sequence to be associated with a specific table column, such
            that if that column (or its whole table) is dropped, the
            sequence will be automatically dropped as well. The specified
            table must have the same owner and be in the same schema as
            the sequence. OWNED BY NONE, the default, specifies that there
            is no such association.
        @not_exists: (#bool) |True| to add |IF NOT EXISTS| clause
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    sequence = Sequence(*args, **kwargs)
    return _cast_return(sequence, dry)


def create_function(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @function: (#str or :class:Function) name of the function with
            arguments e.g. |func(arg1 int, arg2 text)| or
            |Function('func', safe('arg1 int'), safe('arg2 text'))|
        @arg: (#str or :class:BaseExpression) function arguments in the form of
            |[argmode ] [ argname ] argtype [ { DEFAULT | = } default_expr|
        @expression: (#str or :class:BaseExpression or :class:Query)
            the function context. This is the expression that gets executed
            when the function is called.
        @returns: (#str) data type name
        @language: (#str) name of the language that the function is
            implemented in
        @*opt: (#str) option flags to implement in the query after |LANGUAGE|
            e.g. |WINDOW|
        @**opts: |option_name=option_value| pairs to implement in the query
            after |LANGUAGE| e.g. |cost=0.0025|
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    function = Function(*args, **kwargs)
    return _cast_return(function, dry)


def create_type(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the type
        @*opt: (#str or :class:Clause) type options
            * |PASSEDBYVALUE|
        @attrs: (#tuple(#tuple)) #tuple pairs of |(attr_name, data_type, *opt)|
            - creates types from attributes e.g.
              |CREATE TYPE compfoo AS (f1 int, f2 text);|
        @**opts: (#str or :class:Function) type options |name=value|
            * INPUT = input_function,
            * OUTPUT = output_function
            * RECEIVE = receive_function
            * SEND = send_function
            * TYPMOD_IN = type_modifier_input_function
            * TYPMOD_OUT = type_modifier_output_function
            * ANALYZE = analyze_function
            * INTERNALLENGTH = { internallength | VARIABLE }
            * ALIGNMENT = alignment
            * STORAGE = storage
            * LIKE = like_type
            * CATEGORY = category
            * PREFERRED = preferred
            * DEFAULT = default
            * ELEMENT = element
            * DELIMITER = delimiter
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    type = Type(*args, **kwargs)
    return _cast_return(type, dry)


def create_enum_type(orm, name, *types, dry=False):
    """ @orm: (:class:ORM)
        @name: (#str) the name of the type
        @types: (#str) types to create
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor
            is returned
    """
    enum = EnumType(orm, name, *types)
    return _cast_return(enum, dry)


def create_range_type(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the type
        @**opts: (#str or :class:Function) type options |name=value|
            * SUBTYPE = subtype
            * SUBTYPE_OPCLASS = subtype_operator_class
            * COLLATION = collation
            * CANONICAL = canonical_function
            * SUBTYPE_DIFF = subtype_diff_function
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor
            is returned
    """
    range = RangeType(*args, **kwargs)
    return _cast_return(range, dry)


def create_role(orm, name, *options, in_role=None, in_group=None, role=None,
                admin=None, user=None, password=None, password_encryption=True,
                valid_until=None, connlimit=None, dry=False):
    """ @orm: (:class:ORM)
        @name: (#str) name of the role
        @*options: (#str or :class:Clause) names of role options
            * SUPERUSER / NOSUPERUSER
            * CREATEDB / NOCREATEDB
            * CREATEROLE / NOCREATEROLE
            * CREATEUSER / NOCREATEUSER
            * INHERIT / NOINHERIT
            * LOGIN / NOLOGIN
            * REPLICATION / NOREPLICATION
        @in_role: (#str or #tuple) sets the |IN ROLE| option
        @in_group: (#str or #tuple) sets the |IN GROUP| option
        @role: (#str or #tuple) sets the |ROLE| option
        @admin: (#str or #tuple) sets the |ADMIN| option
        @user: (#str or #tuple) sets the |USER| option
        @password: (#str) sets a password for the role
        @password_encryption: (#bool) encrypts @password
        @valid_until: (:class:datetime.datetime) sets the |VALID UNTIL| option
        @connlimit: (#int) sets the |CONNECTION LIMIT| option
        @dry: (#bool) True to execute the query right away

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    role = Role(orm, name, *options, in_role=in_role, in_group=in_group,
                role=role, admin=admin, user=user, password=password,
                password_encryption=password_encryption,
                valid_until=valid_until, connlimit=connlimit)
    return _cast_return(role, dry)


def create_trigger(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the trigger
        @at: (#str or :class:Clause) |BEFORE|, |AFTER| or |INSTEAD OF|;
            determines whether the function is called before or after
            the event.
        @*events: (#str or :class:Clause) one of |INSERT, |UPDATE [OF col]|,
            |TRUNCATE| or |DELETE|; this specifies the event that will fire
            the trigger. Multiple events can be specified.
        @table: (#str) name of the table the trigger is for
        @ref_table: (#str) name of another table referenced by the constraint
        @type: (#str or :class:Clause) |ROW| or |STATEMENT|; specifies whether
            the trigger procedure should be fired once for every row affected.
            by the trigger event, or just once per SQL statement.
        @function: (#str or :class:Function) user-supplied function
            which is executed when the trigger fires.
        @constraint: (#bool) True to include |CONSTRAINT| clause
        @timing: (#str or :class:Clause) one of:
            * |DEFERRABLE|
            * |NOT DEFERRABLE|
            * |INITIALLY IMMEDIATE|
            * |INITIALLY DEFERRED|
        @condition: (#str or :class:Expression) boolean expression that
            determines whether the trigger function will actually be executed
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    trigger = Trigger(*args, **kwargs)
    return _cast_return(trigger, dry)


def create_user(orm, name, *options, in_role=None, in_group=None, role=None,
                admin=None, user=None, password=None, password_encryption=True,
                valid_until=None, connlimit=None, dry=False):
    """ :see::func:create_role """
    user = User(orm, name, *options, in_role=in_role, in_group=in_group,
                role=role, admin=admin, user=user, password=password,
                password_encryption=password_encryption,
                valid_until=valid_until, connlimit=connlimit)
    return _cast_return(user, dry)


def create_view(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the view
        @as_: (:class:Select) query to create a view for
        @columns: (#str or :class:Field) optional names to be used for columns
            of the view. If not given, the column names are deduced from
            the query.
        @security_barrier: (#bool) True to enable WITH (security_barrier)
        @temporary: (#bool) True to create a temporary view
        @materialized: (#bool) True to create materialized view
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    view = View(*args, **kwargs)
    return _cast_return(view, dry)


def create_cast(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @source_type: (#str) name of the source data type of the cast
        @target_type: (#str) name of the target data type of the cast
        @function: (#str or :class:Function) The function used to perform
            the cast e.g. |funcname(argtypes)|
            If no function is supplied, the cast will be created with
            the |WITHOUT FUNCTION| flag.
        @as_assignment: (#bool) True indicates that the cast may be invoked
            implicitly in assignment contexts.
        @as_implicit: (#bool) True indicates that the cast may be invoked
            implicitly in any context.
        @inout: (#bool) indicates that the cast is an I/O conversion cast
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    cast = Cast(*args, **kwargs)
    return _cast_return(cast, dry)


def create_database(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the database
        @owner: (#str) name of the database user who will own the
            new database
        @template: (#str) name of the template from which to create the
            new database
        @encoding: (#str) character set encoding to use in the new database
        @tablespace: (#str) name of the tablespace that will be associated
            with the new database
        @lc_collate: (#str) collation
        @lc_ctype: (#str) ctype
        @connlimit: (#int) how many concurrent connections can be made to
            this database. |-1| (the default) means no limit.
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    database = Database(*args, **kwargs)
    return _cast_return(database, dry)


def create_domain(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the domain
        @data_type: (#str) name of the underlying data type of the domain
        @collate: (#str) collation
        @default: specifies a default value for columns of the
            domain data type
        @constraint: (#str) name for a constraint. If not specified, the
            system generates a name.
        @not_null: (#bool) |True| if values of this domain are not allowed
            to be null.
        @check: (#str or :class:Expression) specify integrity constraints or
            tests which values of the domain must satisfy.
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    domain = Domain(*args, **kwargs)
    return _cast_return(domain, dry)


def create_operator(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the operator to be defined
        @func: (#str) name of the function used to implement this operator
        @hashes: (#bool) True indicates this operator can support a hash join
        @merges: (#bool) True Indicates this operator can support a merge join
        @**opts: |name=(str)value| option pairs to create the operator with
            e.g. |LEFTARG='box', RIGHTARG='box'|
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    op = Operator(*args, **kwargs)
    return _cast_return(op, dry)


def create_rule(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the database
        @event: (#str) one of SELECT, INSERT, UPDATE, or DELETE
        @table: (#str) name of the table the rule applies to
        @command: (#str or :class:Expression) command  that makes up
            the rule action
        @also: (#bool) |True| adds an |ALSO| flag to the |DO| command
        @condition: (#str or :class:BaseExpression) boolean expression
        @instead: (#bool) |True| adds an |INSTEAD| flag to the |DO| command
        @nothing: (#bool) |True| adds a |NOTHING| flag to the |DO| command
        @replace: (#bool) |True| adds a |OR REPLACE| flag to the |CREATE|
            clause
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    rule = Rule(*args, **kwargs)
    return _cast_return(rule, dry)


def create_tablespace(*args, dry=False, **kwargs):
    """ @orm: (:class:ORM)
        @name: (#str) name of the tablespace
        @location: (#str) directory that will be used for the tablespace
        @**options: (#str) |name=val| tablespace parameter to be set or reset.
            Currently, the only available parameters are |seq_page_cost|
            and |random_page_cost|.
        @owner: (#str) user name
        @dry: (#bool) |True| to execute the query before returning

        -> :class:BaseCreator if dry is |False|, otherwise the client cursor is
            returned
    """
    tablespace = Tablespace(*args, **kwargs)
    return _cast_return(tablespace, dry)
