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

    def __init__(self, orm, name, query=None):
        """ `Create a View`
            @orm: (:class:ORM)
            @name: (#str) name of the view
            @query: (:class:Select or a VALUES :class:Clause)
        """
        super().__init__(orm, name)
        self._query = None
        self._columns = None
        self._security_barrier = False
        self._materialized = False
        self._temporary = False

    def set_query(self, query):
        self._query = query

    def columns(self, *columns):
        self._columns = columns

    def security_barrier(self):
        self._security_barrier = True

    def materialized(self):
        self._materialized = True

    def temporary(self):
        self._temporary = True

    @property
    def query(self):
        tmp = ""
        if self._temporary:
            tmp = "TEMP "
        if self._materialized:
            tmp += 'MATERIALIZED '
        security_barrier = _empty
        if self._security_barrier:
            security_barrier = Clause('WITH', safe('security_barrier'), wrap=True)
        columns = list(map(lambda x: x if isinstance(x, Field) else safe(x),
                           self._columns))
        create_clause = Clause('CREATE {}VIEW'.format(tmp),
                               safe(self.name),
                               Clause("", *columns, join_with=", ", wrap=True),
                               security_barrier,
                               use_field_name=True)
        self.orm.state.add(create_clause)
        self.orm.state.add(Clause('AS', self._query))
        query = Raw(self.orm)
        self.orm.reset()
        return query
