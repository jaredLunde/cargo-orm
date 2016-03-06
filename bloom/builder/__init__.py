"""

  `Bloom SQL Builder`
  ``Creates models from tables and tables from models``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2016 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import re
import copy
import docr
import yapf
from collections import OrderedDict

try:
    from cnamedtuple import namedtuple
except ImportError:
    from collections import namedtuple

from vital.cache import cached_property
from vital.debug import prepr, logg, line
from vital.tools import strings as string_tools

from bloom.exceptions import *
from bloom.cursors import CNamedTupleCursor
from bloom.orm import ORM, Model, QueryState
from bloom.expressions import *
from bloom.fields import *
from bloom.statements import *

from bloom.builder.comments import Comment
from bloom.builder.extras import *
from bloom.builder.extensions import *
from bloom.builder.fields import *
from bloom.builder.functions import Function as CreateFunction
from bloom.builder.tables import *
from bloom.builder.indexes import *
from bloom.builder.types import Type, EnumType
from bloom.builder.utils import *

from bloom.builder.create_shortcuts import *
from bloom.builder.drop_shortcuts import *


__all__ = (
    'Modeller',
    'Builder',
    'Build',
    'Column',
    'Table',
    'Index',
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
    'create_view',
    'drop',
    'Drop',
    'drop_creator',
    'drop_cast',
    'drop_database',
    'drop_domain',
    'drop_operator',
    'drop_extension',
    'drop_event_trigger',
    'drop_schema',
    'drop_index',
    'drop_language',
    'drop_materialized_view',
    'drop_model',
    'drop_sequence',
    'drop_function',
    'drop_table',
    'drop_type',
    'drop_role',
    'drop_rule',
    'drop_tablespace',
    'drop_trigger',
    'drop_user',
    'drop_view'
)


_model_tpl = """
class {clsname}(Model):{comment}
    table = '{table}'
    schema = '{schema}'
    ordinal = ({ordinal})
    {fields}
"""


#
# `Modelling data structures`
#


class Modeller(object):

    def __init__(self, orm, *tables, schema=None, banner=None):
        """`Model Builder`

            Generates models from Postgres tables.

            @*tables: (#str) one or several table names. If no table is
                given, the entire @schema will be searched.
            @schema: (#str) name of the table schema/search path to search
        """
        self.orm = orm
        self.orm.set_cursor_factory(CNamedTupleCursor)
        self.tables = tables
        self.schema = schema or orm.schema or orm.db.schema or 'public'
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

    def _format_cutoff(self, com, cutoff, separator=""):
        new_com = []
        length = 0
        for part in com.split(" "):
            length += len(part) + 1
            if length > cutoff:
                part = '\n    ' + separator + part
                length = len(part)
            new_com.append(part)
        return " ".join(new_com)

    def _format_ordinal(self, ordinal):
        com = ordinal
        cutoff = 71
        if len(com) > cutoff:
            com = self._format_cutoff(com, cutoff, '           ')
        return com.lstrip()

    def _format_inline_comment(self, comment):
        com = "#: " + comment
        cutoff = 67
        if len(com) > cutoff:
            com = self._format_cutoff(com, cutoff, '#  ')
        return com.lstrip()

    def _format_block_comment(self, comment):
        com = '""" ' + comment + ' """'
        cutoff = 71
        if len(com) > cutoff:
            com = self._format_cutoff(com, cutoff, '    ')
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
        ordinal = "'{}'".format("', '".join(field.field_name
                                            for field in fields))
        if len(fields) == 1:
            ordinal += ','
        fields = (self.create_field(FieldMeta(self.orm, field, table))
                  for field in fields)
        comment = ""
        if table.comment:
            comment = self._format_block_comment(table.comment)
        return yapf.yapf_api.FormatCode(self.model_tpl.format(
            clsname=clsname,
            ordinal=self._format_ordinal(ordinal),
            table=table.name,
            fields="\n    ".join(fields),
            schema=self.schema,
            comment=comment
        ), style_config="pep8")[0]

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
            banner = "{}\nfrom bloom import Model, ForeignKey\n" +\
                     "from bloom.fields import *\n\n" +\
                     "{}"
        return banner.format(self.banner or "", models)

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


class BuilderItems(object):
    __slots__ = ('_dict', '_type', 'builder')

    def __init__(self, type, items, builder=None):
        self._dict = OrderedDict()
        self._type = type
        for name, item in items:
            self._dict[name] = copy.copy(item)
        self.builder = builder

    @prepr('_dict', _no_keys=True)
    def __repr__(self): return

    def __setitem__(self, name, value):
        self._dict[name] = copy.copy(value)

    def __getitem__(self, name):
        return self._dict[name]

    def __delitem__(self, name):
        del self._dict[name]

    def __getattr__(self, name):
        try:
            return self.__getattribute__(name)
        except AttributeError as e:
            if name not in self._dict:
                raise AttributeError(e)
            return self._dict[name]

    def __setattr__(self, name, value):
        if name not in self.__slots__ and\
          (not name.startswith('__') and not name.endswith('__')):
            self._dict[name] = value
        else:
            BuilderItems.__dict__[name].__set__(self, value)

    def __delattr__(self, name):
        if name not in self.__slots__ and\
          (not name.startswith('__') and not name.endswith('__')):
            del self._dict[name]
        else:
            BuilderItems.__dict__[name].__del__(self, name)

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        for val in self._dict.values():
            yield val

    def add(self, name, *args, **kwargs):
        try:
            setattr(self,
                    name,
                    self._type(self.builder.model, *args, **kwargs))
        except AttributeError:
            setattr(self,
                    name,
                    self._type(*args, **kwargs))

    def remove(self, name):
        del self[name]


class Builder(Table):
    """ ========================================================================
        ``Basic Usage Example``
        ..
            from bloom import Model
            from bloom.fields import *
            from bloom.builder import Builder

            class Users(Model):
                schema = 'shard_0'
                uid = UID()
                username = Username(maxlen=14, index=True, unique=True,
                                    not_null=True)
                email = Email(unique=True, minlen=5, not_null=True)
                password = Password(minlen=8, not_null=True)


            class UsersBuilder(Builder):
                model = Users()
                ordinal = ('uid', 'username', 'email', 'password')

                def after(self):
                    self.comment_on(self.columns.uid,
                                    'A universally unique identifier '+
                                    'implementation.')


            if __name__ == '__main__':
                from bloom import create_client
                #: Creates the ORM client connection
                create_client()
                #: Builds the model
                build = UsersBuilder()
                build.run()
        ..
        |CREATE SCHEMA shard_0;                                               |
        |SET SEARCH PATH shard_0;                                             |
        |CREATE SEQUENCE bloom_uid_seq NOMINVAL NOMAXVAL CACHE 1024;          |
        |CREATE FUNCTION bloom_uid() ...;                                     |
        |CREATE TABLE users (                                                 |
        |   uid      bigint PRIMARY DEFAULT bloom_uid(),                      |
        |   username varchar(14) UNIQUE NOT NULL,                             |
        |   email    varchar(320) UNIQUE NOT NULL                             |
        |               CHECK (char_length(email) > 5),                       |
        |   password text NOT NULL                                            |
        |);                                                                   |
        |CREATE UNIQUE INDEX users_username_unique_index                      |
        |   USING BTREE(username);                                            |
        |COMMENT ON COLUMN users.uid IS 'A universally unique identifier      |
        |   implementation.'                                                  |
    """
    model = None
    ordinal = None

    def __init__(self, model=None, schema=None):
        """ `Table Builder`
             Generates tables, indexes, foreign keys and other constraints,
             comments, functions and schemas for given :class:Model(s).

             @model: (:class:Model) initialized model
             @schema: (#str) schema to use, otherwise the schema defined
                within the @model will be defaulted to
        """
        self.model = model.copy() if model is not None else self.model.copy()
        super().__init__(self.model.copy(), self.model.table)
        self.schema = schema or self.model.schema or \
            self.model.db.schema or 'public'
        self.from_fields(*self.columns)

    def before(self):
        """ Executed immediately before the builder runs. This is where
            you'll want to do things like make edits to your :prop:columns
            and :prop:indexes, and add :meth:constraints.
        """
        pass

    def after(self):
        """ Executed immediately after the bulder runs """
        pass

    def _get_comment_type(self, obj):
        return obj.__class__.__name__.upper()

    def _get_comment_ident(self, obj):
        return obj._common_name

    def comment_on(self, obj, comment, dry=False):
        """ Immediately adds comments to @obj in Postgres unless @dry
            is |True|.

            @obj: (:class:bloom.builder.utils.BaseCreator)
            @comment: (#str) the comment content
            @dry: (#bool) |True| to return the :class:Comment without
                executing the query.
        """
        if isinstance(obj, Field):
            obj = Column(obj)
        elif obj == self.model:
            obj = self
        type = self._get_comment_type(obj)
        ident = self._get_comment_ident(obj)
        return comment_on(self.orm, type, ident, comment, dry=dry)

    _special_fields = {UID: UIDFunction,
                       UUID: UUIDExtension,
                       HStore: HStoreExtension}

    @cached_property
    def comments(self):
        """ -> (:class:BuilderItems mutable namedtuple-like object) of the
                comments which are set to be created by the Builder
                autonomously.
        """
        return BuilderItems(lambda *a, **k: self.comment_on(*a, dry=True, **k),
                            ((field.field_name,
                             self.comment_on(self.columns[field.field_name],
                                             f._get_comment(),
                                             dry=True))
                             for field in self.model.fields
                             for t, f in self._special_fields.items()
                             if isinstance(field, t)))

    @cached_property
    def functions(self):
        """ -> (:class:BuilderItems mutable namedtuple-like object) of the
                functions which are set to be created by the Builder
                autonomously.
        """
        return BuilderItems(CreateFunction,
                            ((f.extras_name, f(self.model))
                             for field in self.model.fields
                             for t, f in self._special_fields.items()
                             if isinstance(field, t) and
                                issubclass(f, CreateFunction)))

    @cached_property
    def extensions(self):
        """ -> (:class:BuilderItems mutable namedtuple-like object) of the
                extensions which are set to be created by the Builder
                autonomously.
        """
        return BuilderItems(Extension,
                            ((f.extras_name, f(self.model))
                             for field in self.model.fields
                             for t, f in self._special_fields.items()
                             if isinstance(field, t) and
                                issubclass(f, Extension)))

    @cached_property
    def columns(self):
        """ -> (:class:BuilderItems mutable namedtuple-like object) of the
                columns which are set to be created by the Builder
                autonomously.
        """
        fields = self.model.fields
        if self.ordinal:
            fields = [self.model.__getattribute__(name)
                      for name in self.ordinal]
        return BuilderItems(Column,
                            ((field.field_name, Column(field))
                             for field in fields), builder=self)

    @cached_property
    def indexes(self):
        """ -> (:class:BuilderItems mutable namedtuple-like object) of the
                indexes which are set to be created by the Builder
                autonomously.
        """
        return BuilderItems(Index,
                            ((field.field_name, Index(self.model, field))
                             for field in self.model.indexes), builder=self)

    def create_schema(self):
        """ Creates the schema for the model if it doesn't exist """
        if self.schema != 'public':
            self.model.schema = 'public'
            try:
                create_schema(self.model, self.schema)
            except QueryError as e:
                logg(e.message).notice()
            self.model.schema = self.schema

    def create_functions(self):
        """ Creates all of the functions defined in :prop:functions """
        for function in self.functions:
            function.execute()

    def create_extensions(self):
        """ Creates all of the functions defined in :prop:functions """
        for extension in self.extensions:
            try:
                extension.execute()
            except QueryError as e:
                logg(e.message).notice()

    def create_indexes(self):
        """ Creates all of the indexes defined in :prop:indexes """
        for index in self.indexes:
            try:
                index.execute()
            except QueryError as e:
                logg(e.message).notice()

    def create_comments(self):
        """ Creates all of the comments in :prop:comments """
        for comment in self.comments:
            comment.execute()

    def debug(self):
        self.from_fields(*self.columns)
        print()
        line('=')
        logg(repr(self.schema) + '.' + repr(self.name.__str__())).log(
            'MODEL:' +
            self.model.__module__ + '.' + self.model.__class__.__name__,
            force=True,
            color='bold')
        line('=')
        logg(self.columns._dict).log('COLUMNS', color='blue', force=True)
        line('-', 'gray')
        logg('\n' + self.query.mogrified).log(
            'CREATE TABLE',
            color='boldblue',
            force=True)
        line('-', 'gray')
        logg(self.indexes._dict).log('INDEXES', color='blue', force=True)
        line('-', 'gray')
        logg(self.extensions._dict).log('EXTENSIONS', color='blue', force=True)
        line('-', 'gray')
        logg(self.functions._dict).log('FUNCTIONS', color='blue', force=True)
        line('-', 'gray')
        logg(self.comments._dict).log('COMMENTS', color='blue', force=True)
        line('—', 'gray')
        return self

    def run(self):
        cn = self.model.__class__.__name__
        logg().log('Building `%s` at `%s.%s`...' %
                   (cn, self.schema, self.name))
        self.before()
        self.from_fields(*self.columns)
        self.create_schema()
        self.create_extensions()
        self.create_functions()
        try:
            self.execute()
        except QueryError as e:
            raise BuildError('Error building `{}`: {}'.format(cn, e.message))
        self.create_indexes()
        self.create_comments()
        self.after()


# TODO: Build shards
# NOTE: http://www.craigkerstiens.com/2012/11/30/sharding-your-database/
#    hash(user_id) % 4096

class Build(object):
    """ ========================================================================
        ``Usage Example``
        ..
            from bloom.builder import Builder, Build, create_tables


            class UsersBuilder(Builder):
                model = Users()


            class PostsBuilder(Builder):
                model = Posts()


            if __name__ == '__main__':
                # Build from imported path
                Build('__main__').run()
                # is the same as
                Build.main()
                # in this case is the same as
                app_build_alt = Build(PostsBuilder(),
                                      UsersBuilder())
                app_build_alt.run()
                # is the same as
                create_tables('__main__')
        ..
    """
    def __init__(self, *builders):
        """ Finds all of the :class:Builder objects in @path or optionally
            builds all of the models in @*builders. It is of necessity that
            the @builders are ordered properly if there are dependencies.

            @*builders: (:class:Builder or #str) one or several
                initialized :class:Builder(s) or a #str importable python
                path to where one or several :class:Builder(s) are located,
                e.g. |cool_app.models.builders.users|. They will be run
                alphabetically if imported via a string.
        """
        self.builders = builders

    def get_builders(self, path):
        return [obj.obj()
                for name, obj in docr.Docr(path).classes.items()
                if issubclass(obj.obj, Builder)]

    def _run_builders(self, builders, debug=False):
        for builder in builders:
            if isinstance(builder, str):
                builder = docr.Docr(builder)
                if builder.type == builder.MODULE:
                    self._run_builders(self.get_builders(builder.obj),
                                       debug=debug)
            elif debug:
                builder.debug()
            else:
                builder.run()

    def debug(self):
        self._run_builders(self.builders, debug=True)
        return self

    def run(self):
        self.before()
        self._run_builders(self.builders)
        self.after()

    def before(self):
        """ Executed immediately before the builder runs. This is where
            you'll want to do things like make edits to your :prop:columns
            and :prop:indexes, and add :meth:constraints.
        """
        pass

    def after(self):
        """ Executed immediately after the bulder runs """
        pass

    @staticmethod
    def main():
        return Build('__main__').run()


def create_models(orm, *tables, banner=None, schema='public',
                  output_to=str, **kwargs):
    modeller = Modeller(orm, *tables, banner=banner, schema=schema)
    return modeller.run(output_to=output_to, **kwargs)


def create_tables(*builders, dry=False):
    """ :see::class:Build """
    build = Build(*builders)
    if not dry:
        return build.run()
    return build


if __name__ == '__main__':
    # TODO: CLI
    pass
