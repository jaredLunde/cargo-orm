#!/usr/bin/python3 -S
"""

  `Bloom ORM Types Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from vital.tools.dicts import merge_dict

from bloom.fields.field import Field
from bloom.expressions import *
from bloom.statements import *
from bloom.builder.utils import BaseCreator


__all__ = ('Type', 'RangeType', 'EnumType')


class Type(BaseCreator):

    def __init__(self, orm, name, *opt, attrs=None, **opts):
        """ `Create a Type`
            :see::func:bloom.builders.create_type
        """
        super().__init__(orm, name)
        self._options = None
        if attrs:
            self.as_attrs(*attrs)
        if opt:
            opt = {str(o): True for o in opt}
            opts = merge_dict(opt, opts)
        if opts:
            self.options(**opts)

    def _cast(self, x):
        if isinstance(x, Field):
            return safe(x.field_name)
        else:
            return self._cast_safe(x)

    def as_attrs(self, *attrs):
        options = [Clause('', *map(self._cast, opt))
                   for opt in attrs]
        self._options = ValuesClause('AS', *options)
        print(self._options)
        return self

    def options(self, **opt):
        opt = [safe(k.upper().replace("_", " ")).eq(self._cast_safe(v))
               if v is not True else
               safe(k.upper())
               for k, v in opt.items()]
        self._options = ValuesClause("", *opt)
        return self

    @property
    def query(self):
        '''
        CREATE TYPE name AS
            ( [ attribute_name data_type [ COLLATE collation ] [, ... ] ] )

        CREATE TYPE name (
            INPUT = input_function,
            OUTPUT = output_function
            [ , RECEIVE = receive_function ]
            [ , SEND = send_function ]
            [ , TYPMOD_IN = type_modifier_input_function ]
            [ , TYPMOD_OUT = type_modifier_output_function ]
            [ , ANALYZE = analyze_function ]
            [ , INTERNALLENGTH = { internallength | VARIABLE } ]
            [ , ALIGNMENT = alignment ]
            [ , STORAGE = storage ]
            [ , LIKE = like_type ]
            [ , CATEGORY = category ]
            [ , PREFERRED = preferred ]
            [ , DEFAULT = default ]
            [ , ELEMENT = element ]
            [ , DELIMITER = delimiter ]
        )
        '''
        self.orm.reset()
        self._add(Clause('CREATE TYPE', self.name),
                  self._options)
        return Raw(self.orm)


class RangeType(Type):

    def __init__(self, *args, **kwargs):
        """ `Create a Range Type`
            :see::func:bloom.builders.create_type
        """
        super().__init__(*args, **kwargs)

    def options(self, **opt):
        opt = [safe(k).eq(self._cast_safe(v)) if v is not True else Clause(k)
               for k, v in opt.items()]
        self._options = ValuesClause("AS RANGE", *opt)
        return self


class EnumType(BaseCreator):

    def __init__(self, orm, name, *types):
        """ `Create an Enumerated Type`
            :see::class:BaseCreator
            @types: (#str) types to create
        """
        super().__init__(orm, name)
        self.types = list(types)

    def add_type(self, *types):
        self.types.extend(types)

    @property
    def query(self):
        self.orm.reset()
        types = tuple(self.types)
        clause = Clause('CREATE TYPE',
                        safe(self.name),
                        Clause("AS", types))
        self.orm.state.add(clause)
        return Raw(self.orm)
