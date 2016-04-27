"""

  `Cargo SQL Builder Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/cargo-orm

"""
import sys
import re

from vital.debug import prepr
from vital.cache import cached_property

from cargo import aliased
from cargo.etc.translator import postgres
from cargo.etc import types
from cargo.expressions import *
from cargo.relationships import Reference
from cargo.fields import Encrypted, Serial, BigSerial, SmallSerial, Text,\
                         Slug, Password, Username

from cargo.builder.utils import *


__all__ = (
    'FieldMeta',
    '_field_tpl',
    'find_column',
    'Column',
    'ArrayColumn',
    'EnumColumn',
    'EncryptedColumn',
    'IntColumn',
    'CharColumn',
    'SerialColumn',
    'NumericColumn')


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
        return _get_all_args(self.cls)

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
        elif name == 'decimal_places':
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


class Column(BaseCreator):

    def __init__(self, field, translator=postgres, check=_empty,
                 not_null=_empty, unique=_empty, primary=_empty,
                 default=_empty, references=_empty, timing=_empty,
                 parameters=_empty, data_type=_empty, typed=_empty):
        """`Column wrapper for :class:Field(s)`

            @field: (:class:Field)
            @check: (#str or :class:BaseExpression) check constraint is the
                most generic constraint type. It allows you to specify that the
                value in a certain column must satisfy a Boolean (truth-value)
                expression
            @not_null: (#bool) |True| if the column cannot be null
            @unique: (#bool or #str or :class:Clause) |True| to set plain
                |UNIQUE| constraint #str or :class:Clause to set
                parameterized constraint
            @primary: (#bool or #str or :class:Clause) |True| to set plain
                |PRIMARY KEY| constraint #str or :class:Clause to set
                parameterized constraint
            @default: (#str|#int|#bool or #list or :class:BaseExpression)
                sets the default value constraint
            @references: (#str or :class:BaseExpression) sets the |REFERENCES|
                clause to the constraints provided
            @timing: (#str or :class:Clause) one of:
                * |DEFERRABLE|
                * |NOT DEFERRABLE|
                * |INITIALLY IMMEDIATE|
                * |INITIALLY DEFERRED|
            @translator: field type translator for converting cargo
                :class:Field objects to sql types
            @parameters: (#str or :class:BaseExpression) column data type
                parameters
            @data_type: (#str or :class:BaseExpression) column data type
            @typed: (#bool) |True| if this is for a typed table
        """
        self._name = field.field_name
        self._clauses = []
        self.field = field
        self._parameters = parameters
        self._datatype = data_type
        self.translator = translator

        self._not_null = None
        nn = not_null if not_null is not _empty else field.not_null
        if nn:
            self.not_null()

        self._typed = None
        if typed:
            self.typed()

        self._default = None
        default = default if default is not _empty else field.default
        if default is not None:
            self.default(default)

        self._check = None
        if check:
            self.check(check)

        self._unique = None
        unique = unique if unique is not _empty else field.unique
        if unique:
            unique = [unique] if not isinstance(unique, (tuple, list)) else \
                unique
            self.unique(*unique)

        self._primary = None
        primary = primary if primary is not _empty else field.primary
        if primary:
            primary = [primary] if not isinstance(primary, (tuple, list)) else\
                primary
            self.primary(*primary)

        self._references = None
        if references is not _empty:
            references = references
        elif hasattr(field, 'ref'):
            references = field.ref
        if references:
            self.references(references)

        self._timing = None
        if timing:
            self.timing(timing)

        self._add_constraints()

    def set_type(self, data_type):
        """ Sets the column data type to @data_type """
        self._datatype = safe(data_type)

    def typed(self):
        """ Puts the field options in a |WITH OPTIONS| clause """
        self._typed = True
        return self

    def not_null(self):
        """ Adds a |NOT NULL| constraint to the column """
        self._not_null = Clause('NOT NULL')
        return self

    def nullable(self):
        """ Removes the |NOT NULL| constraint from the column if there is
            one
        """
        self._not_null = None
        return self

    def default(self, val=None):
        """ Sets the |DEFAULT| column value to @val. If @val is |None|,
            any default value currently set will be removed.
        """
        if val is None:
            self._default = None
        else:
            self._default = Clause('DEFAULT', val)
        return self

    def check(self, expression):
        """ @expression: (#str or :class:cargo.BaseExpression or None) """
        if expression is None:
            self._check = None
        else:
            self._check = WrappedClause('CHECK', self._cast_safe(expression))
        return self

    def unique(self, *params):
        """ @params: (#str or :class:cargo.BaseExpression or None) UNIQUE
                constraint parameter, if |None| is passed to this method, e.g.,
                |col.unique(None)|, the unique constraint applied to the
                column will be removed
        """
        if len(params) == 1 and params[0] is True:
            params = [_empty]
        elif len(params) == 1 and params[0] is None:
            params = None
        else:
            params = map(self._cast_safe, params)
        if params is not None:
            self._unique = Clause('UNIQUE', *params)
        else:
            self._unique = None
        return self

    def primary(self, *params):
        """ @params: (#str or :class:cargo.BaseExpression or None) PRIMARY
                constraint parameter, if |None| is passed to this method,
                e.g., |col.primary(None)|, the primary constraint applied
                to the column will be removed
        """
        if len(params) == 1 and params[0] in {True}:
            params = [_empty]
        elif len(params) == 1 and params[0] is None:
            params = None
        else:
            params = map(self._cast_safe, params)
        if params is not None:
            self._primary = Clause('PRIMARY KEY', *params)
        else:
            self._primary = None
        return self

    def timing(self, when):
        if when is None:
            self._timing = None
        else:
            self._timing = Clause(str(when))
        return self

    def _get_col_ref(self, ref):
        field = self.field
        return Clause('REFERENCES',
                      Function(ref.field.table,
                               safe(ref.field.field_name)),
                      *field.ref.constraints)

    def references(self, val, on_delete=None, on_update=None):
        """ @val: (:class:cargo.relationships.Reference or None or #str) If |None|
                is passed to this method, e.g., |col.references(None)|,
                the constraint applied to the column will be removed if
                it exists
            :see::meth:Table.foreign_key
        """
        if isinstance(val, Reference):
            self._references = self._get_col_ref(val)
        elif val is None:
            self._references = None
        else:
            params = [self._cast_safe(val)]
            if on_delete:
                on_delete = Clause('ON DELETE', self._cast_safe(on_delete))
                params.append(on_delete)
            if on_update:
                on_update = Clause('ON UPDATE', self._cast_safe(on_update))
                params.append(on_update)
            self._references = Clause('REFERENCES', *params)
        return self

    def _add(self, *clauses):
        self._clauses = []
        self._clauses.extend(filter(lambda x: x is not None, clauses))

    @property
    def _common_name(self):
        return self.field.name

    @property
    def OID(self):
        return self.field.OID

    @property
    def raw_datatype(self):
        try:
            return self._datatype or self.field.type_name
        except AttributeError:
            return self.translator.OID_map[self.OID]

    @property
    def datatype(self):
        return safe(self.raw_datatype)

    def _get_constraint_ops(self, _constraints, _ops, _validate):
        ops = None
        for constraint, op, validate in zip(_constraints, _ops, _validate):
            if not hasattr(self.field, constraint):
                continue
            constraint = getattr(self.field, constraint)
            if constraint is not None and constraint is not _empty and\
               validate(constraint):
                if ops is None:
                    ops = op(constraint)
                else:
                    ops = ops.also(op(constraint))
        return ops

    def _add_constraints(self):
        pass

    @property
    def params(self):
        return safe('')

    @property
    def expression(self):
        opt = (
            self.datatype,
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


class ArrayColumn(Column):
    @property
    def raw_datatype(self):
        if self._datatype:
            return safe(self._datatype)
        field = self.field
        maxlen = field.maxlen
        dims = "[]"
        if field.dimensions > 1:
            maxlen = field.maxlen
            dims = ''.join('[]' for _ in range(field.dimensions))
        tfield = field.type.copy()
        tfield.field_name = field.field_name
        tfield.table = field.table
        col = find_column(tfield)
        return "{}{}".format(col.datatype, dims)

    @property
    def datatype(self):
        return safe(self.raw_datatype)

    def _add_constraints(self):
        if self._check is None:
            ftype = Function('array_length', self.field, 1)
            _ops = lambda x: ftype.ge(x), lambda x: ftype.le(x)
            _validate = lambda x: x > 1, lambda x: x > 0
            ops = self._get_constraint_ops(('minlen', 'maxlen'),
                                           _ops,
                                           _validate)
            if ops is not None:
                self._check = WrappedClause('CHECK', ops, use_field_name=True)


class EnumColumn(Column):
    @property
    def raw_datatype(self):
        if self._datatype:
            return self._datatype
        return self.field.type_name

    @property
    def datatype(self):
        return safe(self.raw_datatype)


class EncryptedColumn(Column):
    pass


class CharColumn(Column):

    def _add_constraints(self):
        if self._check is None:
            ftype = Function('char_length', self.field)
            _ops = lambda x: ftype.ge(x), lambda x: ftype.le(x)
            _validate = lambda x: x > 1,\
                        lambda x: x > -1 and self.field.OID != types.VARCHAR
            ops = None
            ops = self._get_constraint_ops(('minlen', 'maxlen'),
                                           _ops,
                                           _validate)
            if ops is not None:
                self._check = WrappedClause('CHECK', ops, use_field_name=True)


class IntColumn(Column):

    def _add_constraints(self):
        if self._check is None:
            _ops = lambda x: self.field.ge(x), lambda x: self.field.le(x)
            _cast = self.field.__class__()
            _validate = lambda x: x != self.field.MINVAL,\
                        lambda x: x != self.field.MAXVAL
            ops = self._get_constraint_ops(('minval', 'maxval'),
                                           _ops,
                                           _validate)
            if ops is not None:
                self._check = WrappedClause('CHECK', ops, use_field_name=True)

class SerialColumn(Column):
    @property
    def datatype(self):
        if self.OID == types.SMALLINT:
            return safe('smallserial')
        elif self.OID == types.INT:
            return safe('serial')
        elif self.OID == types.BIGINT:
            return safe('bigserial')


class NumericColumn(IntColumn):
    @property
    def datatype(self):
        prec = ''
        if self.field.OID == types.DECIMAL:
            d = []
            if self.field.digits and self.field.digits > 0:
                d = [str(self.field.digits)]
                if self.field.decimal_places > -1:
                    d.append(str(self.field.decimal_places))
                prec = '(%s)' % ', '.join(d)
        if self.field.decimal_places and self.field.decimal_places > -1:
            if 16 > self.field.decimal_places > 6:
                return safe('double precision')
            elif self.field.decimal_places > 15:
                return safe('decimal')
        return safe('%s%s' % (self.raw_datatype, prec))


def find_column(field, translator=postgres):
    if isinstance(field, (Serial, BigSerial, SmallSerial)):
        # Serial
        return SerialColumn(field, translator)
    if isinstance(field, (Encrypted)):
        # Encrypted
        return EncryptedColumn(field, translator)
    elif field.OID in types.category.ARRAYS:
        # Array
        return ArrayColumn(field, translator)
    elif field.OID == types.ENUM:
        # Enum
        return EnumColumn(field, translator)
    elif field.OID in types.category.CHARS:
        # Character
        return CharColumn(field, translator)
    elif field.OID in types.category.INTS:
        # Integer
        return IntColumn(field, translator)
    elif field.OID in types.category.NUMERIC:
        # Numeric
        return NumericColumn(field, translator)
    else:
        # Default
        return Column(field, translator)
