#!/usr/bin/python3 -S
"""

  `Bloom ORM Types Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from bloom.fields.field import Field
from bloom.expressions import *
from bloom.statements import *
from bloom.builder.utils import BaseCreator


__all__ = ('Type', 'EnumType')


class Type(BaseCreator):
    pass


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
        types = tuple(self.types)
        clause = Clause('CREATE TYPE',
                        safe(self.name),
                        Clause("AS", types))
        self.orm.state.add(clause)
        query = Raw(self.orm)
        self.orm.reset()
        return query
