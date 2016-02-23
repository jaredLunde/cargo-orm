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
from bloom.builder.types import Type, EnumType
from bloom.builder.utils import *

from bloom.builder.create_shortcuts import *
from bloom.builder.drop_shortcuts import *


__all__ = (
    'Modeller',
    'Builder',
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
    'create_view'
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


class Build(object):
    pass


class Builder(object):

    def __init__(self, *models, schema=None):
        """ `Table Builder`

             Generates the tables for given models.
        """
        self.models = models
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


if __name__ == '__main__':
    # TODO: CLI
    pass
