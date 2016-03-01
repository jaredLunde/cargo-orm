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
import copy
import yapf
from collections import OrderedDict

try:
    from cnamedtuple import namedtuple
except ImportError:
    from collections import namedtuple

from vital.cache import cached_property
from vital.debug import prepr, logg
from vital.tools import strings as string_tools

from bloom.exceptions import SchemaError
from bloom.cursors import CNamedTupleCursor
from bloom.orm import ORM, Model, QueryState
from bloom.expressions import *
from bloom.statements import *

from bloom.builder.fields import *
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
    'drop_cast',
    'drop_domain',
    'drop_database',
    'drop_operator',
    'drop_extension',
    'drop_schema',
    'drop_index',
    'drop_sequence',
    'drop_function',
    'drop_tablespace',
    'drop_trigger',
    'drop_type',
    'drop_enum_type',
    'drop_role',
    'drop_rule',
    'drop_user',
    'drop_view'
)


_model_tpl = """
class {clsname}(Model):{comment}
    table = '{table}'
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

    '''def _format_field(self, field):
        cutoff = 71
        if len(field) > cutoff:
            lines = field.split('\n')
            comment = '\n'.join(lines[:-1])
            field = lines[-1]
            sep = ' '.join('' for _ in range(field.lstrip().find('(') + 2))
            field = self._format_cutoff(field, cutoff, sep)
            if comment != field:
                field = '\n'.join((comment, field))
        return field.lstrip()'''

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
            banner = "#!/usr/bin/python3\n" +\
                     "# -*- coding: utf-8 -*-\n" +\
                     "{}\nfrom bloom import Model, ForeignKey\n" +\
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
    __slots__ = ('_dict', 'builder')

    def __init__(self, items, builder=None):
        self._dict = OrderedDict()
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
        setattr(self, name, Index(self.builder.model, *args, **kwargs))

    def remove(self, name):
        del self[name]


class Builder(Table):
    model = None
    ordinal = None

    def __init__(self, model=None, schema=None):
        """ `Table Builder`

             Generates the tables for given models.
        """
        self.model = model or self.model
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

    def comment_on(self, obj):
        # TODO: allow comments on fields/tables/schemas
        # NOTE: http://www.postgresql.org/docs/9.1/static/sql-comment.html
        pass

    @cached_property
    def columns(self):
        fields = self.model.fields
        if self.ordinal:
            fields = [self.model.__getattribute__(name)
                      for name in self.ordinal]
        return BuilderItems(((field.field_name, Column(field))
                            for field in fields), builder=self)

    @cached_property
    def indexes(self):
        return BuilderItems(((field.field_name, Index(self.model, field))
                            for field in self.model.indexes), builder=self)

    def create_schema(self):
        if self.schema != 'public':
            create_schema(self.model, self.schema)

    def debug(self):
        logg(self.name).log(self.model.__module__ + '.' +
                            self.model.__class__.__name__)
        logg(self.columns._dict).log('COLUMNS', color='blue')
        logg(self.indexes._dict).log('INDEXES', color='blue')
        logg(self).log()

    def run(self):
        print()
        self.debug()
        self.before()
        self.from_fields(*self.columns)
        self.create_schema()
        self.after()


class Build(object):

    def __init__(self, *builders, recursive=False):
        """ Finds all of the :class:Builder objects in @path or optionally
            builds all of the models in @*builders

            @*builders: (:class:Builder|#str) one or several :class:Builder
                or a #str importable python path to where the model is located,
                e.g. |cool_app.models.builders.User|
            @recursive: (#bool) |True| to recursively search @path for
                :class:Builder(s)
        """

    def run(self):
        for builder in self.builders:
            builder.run()


def create_models(orm, *tables, banner=None, schema='public',
                  output_to=str, **kwargs):
    modeller = Modeller(orm, *tables, banner=banner, schema=schema)
    return modeller.run(output_to=output_to, **kwargs)


def create_tables(model):
    pass


if __name__ == '__main__':
    # TODO: CLI
    pass
