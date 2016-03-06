"""

  `Bloom SQL Builder Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/bloom-orm

"""
import re

from vital.debug import prepr
from vital.cache import cached_property

from bloom import aliased
from bloom.etc.translator import postgres
from bloom.builder.utils import *


__all__ = ('FieldMeta', '_field_tpl')


_field_tpl = """
    {field_comment}
    {field_name} = {field_cls}({field_args})
"""


class FieldMeta(object):
    ip_re = re.compile(r"""(\b|_)ip(\b|address)""")

    def __init__(self, orm, field, table, translator=None):
        self.orm = orm
        self.field = field
        self.field_name = field.field_name
        self.table = table
        self.translator = translator or postgres
        self.cls = _find_sql_field(field.datatype, field.udtype,
                                   field.category)
        if self.cls == 'Inet' and self.ip_re.search(self.field_name):
            self.cls = 'IP'

    @prepr('field_name', 'table', 'cls')
    def __repr__(self): return

    @property
    def udtype_query(self):
        return _get_sql_file('get_udtypes')

    def get_enumtype(self):
        q = self.orm.execute(self.udtype_query, {'type': self.field.udtype})
        return tuple(r.label for r in q.fetchall())

    @property
    def positional_args(self):
        field_meta = _get_docr(self.cls)
        return field_meta.get_all_args()

    @cached_property
    def foreign(self):
        for key in self.table.foreign_keys:
            if key.field_name == self.field_name:
                return key

    def _get_special_args(self):
        """ ENUM type, ARRAY cast """
        if self.cls == 'Array':
            cast = aliased('str')
            if 'int' in self.field.udtype:
                cast = aliased('int')
            elif 'float' in self.field.udtype:
                cast = aliased('float')
            elif 'bool' in self.field.udtype:
                cast = aliased('bool')
            type = self.translator.translate_from(
                self.field.udtype.replace('_', '', 1),
                self.field.udtype.replace('_', '', 1))
            yield ('type', type)
            yield ('cast', cast)
        elif self.cls == 'Enum':
            yield ('types', self.get_enumtype())

    @cached_property
    def args(self):
        kwargs = []
        add_kwarg = kwargs.append
        for arg, val in self._get_special_args():
            add_kwarg((arg, val))
        for arg, default in self.positional_args.items():
            for attr in self.field._fields:
                real_attr = attr[4:]
                if real_attr == arg and attr.startswith('arg_'):
                    val = getattr(self.field, attr)
                    val = self._cast_arg(real_attr, val)
                    if val is not None:
                        add_kwarg((real_attr, val))
        for index in self.table.indexes:
            if self.field_name in index.fields:
                if index.primary:
                    add_kwarg(('primary', True))
                elif index.unique:
                    add_kwarg(('unique', True))
                else:
                    add_kwarg(('index', True))
        return kwargs

    def _cast_default(self, val):
        if val is None:
            return None
        elif '(' in val and val.endswith(')'):
            func, *args = val.split('(')
            args = '('.join(args).rstrip(')')
            if len(args):
                args = ', safe("""{}""")'.format(args)
            return 'Function("{}"{})'.format(func, args)
        elif self.field.category == 'B':
            if val == 'false' or val == 'f' or val == '0' or val == 'FALSE':
                return False
            return True
        elif self.field.category == 'S':
            return val
        elif self.field.category == 'N':
            if self.cls in {'Float', 'Decimal', 'Numeric'}:
                return float(val)
            else:
                return int(val)
        else:
            return 'safe("""{}""")'.format(val)

    def _cast_arg(self, name, val):
        if name == 'default':
            return self._cast_default(val)
        elif name == 'not_null' and not val:
            return None
        elif name == 'digits':
            if self.field.datatype == 'real':
                return 6
            elif self.field.datatype == 'double precision':
                return 15
            return None
        if not isinstance(val, str):
            return val
        return "'{}'".format(val)

    @property
    def args_string(self):
        return ", ".join(
            '{}={}'.format(k, v)
            for k, v in self.args)
