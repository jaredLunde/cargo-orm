#!/usr/bin/python3 -S
"""

  `Bloom ORM Schema Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from bloom.fields.field import Field
from bloom.expressions import *
from bloom.statements import *
from bloom.builder.utils import BaseCreator


__all__ = ('Schema',)


class Schema(BaseCreator):

    def __init__(self, orm, name, authorization=None, not_exists=True):
        """ `Create an Enumerated Type`
            :see::class:BaseCreator
            @authorization: (#str) username to create a schema for
            @not_exists: (#bool) adds |IF NOT EXISTS| clause to the statement
        """
        super().__init__(orm, name)
        self._authorization = authorization
        self._not_exists = not_exists

    def authorization(self, val):
        self._authorization = val

    def not_exists(self, val):
        self._not_exists = val

    @property
    def query(self):
        auth, ine = _empty, _empty
        if self._authorization:
            auth = Clause('AUTHORIZATION', safe(self._authorization))
        if self._not_exists:
            ine = Clause('IF NOT EXISTS')
        self.orm.state.add(Clause('CREATE SCHEMA', ine, auth, safe(self.name)))
        query = Raw(self.orm)
        self.orm.reset()
        return query
