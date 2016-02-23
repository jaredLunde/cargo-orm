#!/usr/bin/python3 -S
"""

  `Bloom ORM Views Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
from bloom.fields.field import Field
from bloom.expressions import *
from bloom.statements import *
from bloom.builder.utils import BaseCreator


__all__ = ('View',)


class View(BaseCreator):

    def __init__(self, orm, name, *columns, query=None, security_barrier=False,
                 materialized=False, temporary=False):
        """ `Create a View`
            :see::bloom.builders.create_view
        """
        super().__init__(orm, name)
        self._query = query
        self.columns(*columns)
        self._security_barrier = security_barrier
        self._materialized = materialized
        self._temporary = temporary

    def set_query(self, query):
        self._query = query

    def columns(self, *columns):
        self._columns = tuple(self._cast_safe(col) for col in columns)

    def security_barrier(self):
        self._security_barrier = True

    def materialized(self):
        self._materialized = True

    def temporary(self):
        self._temporary = True

    @property
    def query(self):
        self.orm.reset()
        tmp = ""
        if self._temporary:
            tmp = "TEMP "
        if self._materialized:
            tmp += 'MATERIALIZED '
        security_barrier = _empty
        if self._security_barrier:
            security_barrier = Clause('WITH',
                                      safe('security_barrier'),
                                      wrap=True)
        create_clause = Clause('CREATE {}VIEW'.format(tmp),
                               safe(self.name),
                               Clause("", *self._columns, join_with=", ",
                                      wrap=True),
                               security_barrier,
                               use_field_name=True)
        self.orm.state.add(create_clause)
        self.orm.state.add(Clause('AS', self._query))
        return Raw(self.orm)
