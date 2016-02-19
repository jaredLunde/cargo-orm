#!/usr/bin/python3 -S
"""

  `Bloom ORM User Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from bloom.fields.field import Field
from bloom.expressions import *
from bloom.statements import *
from bloom.builder.utils import BaseCreator


__all__ = ('User',)


class User(BaseCreator):

    def __init__(self, orm, name, *types):
        """ `Create an Enumerated Type`
            :see::class:BaseCreator
            @types: (#str) types to create
        """
        super().__init__(orm, name)
        self.types = list(types)
