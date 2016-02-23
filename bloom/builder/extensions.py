#!/usr/bin/python3 -S
"""

  `Bloom ORM Extension Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from bloom.fields.field import Field
from bloom.expressions import *
from bloom.statements import *
from bloom.builder.utils import BaseCreator


__all__ = ('Extension',)


class Extension(BaseCreator):

    def __init__(self, orm, name, schema=None, version=None, old_version=None,
                 not_exists=True):
        """ `Create an Extension`
            :see::func:bloom.builders.create_extension
        """
        super().__init__(orm, name)
        self._schema = None
        if schema:
            self.schema(schema)

        self._version = None
        if version:
            self.version(version)

        self._old_version = None
        if old_version:
            self.old_version(old_version)

        self._not_exists = None
        if not_exists:
            self.not_exists()

    def not_exists(self):
        self._not_exists = Clause('IF NOT EXISTS')
        return self

    def schema(self, name):
        self._schema = Clause('SCHEMA', self._cast_safe(name))
        return self

    def version(self, version):
        self._version = Clause('VERSION', self._cast_safe(version))
        return self

    def old_version(self, version):
        self._old_version = Clause('FROM', self._cast_safe(version))
        return self

    @property
    def parameters(self):
        params = []
        if self._schema:
            params.append(self._schema)
        if self._version:
            params.append(self._version)
        if self._old_version:
            params.append(self._old_version)
        if params:
            return Clause('WITH', *params)

    @property
    def query(self):
        '''
        CREATE EXTENSION [ IF NOT EXISTS ] extension_name
            [ WITH ] [ SCHEMA schema_name ]
                     [ VERSION version ]
                     [ FROM old_version ]
        '''
        self.orm.reset()
        self._add(Clause('CREATE EXTENSION', self._not_exists, self.name),
                  self.parameters)
        return Raw(self.orm)
