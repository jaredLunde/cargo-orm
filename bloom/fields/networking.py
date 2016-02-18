#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Vital SQL Networking Fields`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/VitalSQL

"""
import psycopg2.extras

from vital.debug import prepr

from vital.sql.etc.types import *
from vital.sql.expressions import *
from vital.sql.fields.field import Field

# TODO: 'CIDR': 'cidr'
# TODO: 'MACADDR': 'macaddr'

__slots__ = ('IP', 'Inet')


class IP(Field, StringLogic):
    """ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        Field object for the PostgreSQL field type |INET|.
    """
    __slots__ = (
        'field_name', 'primary', 'unique', 'index', 'notNull', 'value',
        'validation', 'validation_error', '_alias', '_default', 'table',
        '_request')
    sqltype = IP
    current = -1

    def __init__(self, value=None, request=None, default=None, **kwargs):
        """ `IP Address`
            :see::meth:Field.__init__
            @request: Django, Flask or Bottle-like request object
        """
        self._default = default
        self._request = request
        super().__init__(**kwargs)
        self.__call__(value)

    @prepr('name', 'value')
    def __repr__(self): return

    def __call__(self, value=Field.empty):
        if value is not Field.empty:
            if value == self.current:
                value = self.request_ip
                self._set_value(psycopg2.extras.Inet(value))
            elif value is None:
                self._set_value(value)
            else:
                self._set_value(psycopg2.extras.Inet(value))
        return self.value

    @property
    def request_ip(self):
        if self._request is None:
            return None
        if hasattr(self._request, 'remote_addr'):
            return self._request.remote_addr
        elif hasattr(self._request, 'META'):
            x_forwarded_for = self._request.META.get('HTTP_X_FORWARDED_FOR')
            if x_forwarded_for:
                return x_forwarded_for.split(',')[-1].strip()
            else:
                return self._request.META.get('REMOTE_ADDR')
        return None

    @property
    def default(self):
        if self._default == self.current:
            return psycopg2.extras.Inet(self.request_ip)

    def __getstate__(self):
        return dict(
            (slot, getattr(self, slot))
            for slot in self.__slots__
            if hasattr(self, slot)
        )

    def __setstate__(self, state):
        for slot, value in state.items():
            setattr(self, slot, value)

    def copy(self, *args, **kwargs):
        cls = self.__class__(*args, **kwargs)
        cls.field_name = self.field_name
        cls.primary = self.primary
        cls.unique = self.unique
        cls.index = self.index
        cls.notNull = self.notNull
        if self.value is not None and self.value is not self.empty:
            cls.value = copy.copy(self.value)
        cls.table = self.table
        cls.validation = self.validation
        cls.validation_error = self.validation_error
        cls._alias = self._alias
        cls._default = self._default
        cls._request = self._request
        return cls

    __copy__ = copy


Inet = IP
