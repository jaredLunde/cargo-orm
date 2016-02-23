#!/usr/bin/python3 -S
"""

  `Bloom ORM Sequence Builder`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2015 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
import sys

from bloom.expressions import *
from bloom.statements import *
from bloom.builder.utils import BaseCreator


__all__ = ('Sequence',)


class Sequence(BaseCreator):

    def __init__(self, orm, name, incr=None, minval=None, maxval=sys.maxsize,
                 start=None, cache=None, cycle=False, owned_by=None,
                 temporary=False, not_exists=True):
        """ `Create a Sequence`
            :see::func:bloom.builder.create_sequence
        """
        super().__init__(orm, name)
        self.orm = orm
        self._incr = None
        if incr:
            self.incr(incr)
        self.minval(minval)
        self.maxval(maxval)
        self._start = None
        if start:
            self.start(start)
        self._cache = None
        if cache:
            self.cache(cache)
        self._cycle = None
        if cycle:
            self.cycle()
        self.owned_by(owned_by)
        self._temporary = None
        if temporary:
            self.temporary()
        self._not_exists = None
        if not_exists:
            self.not_exists()

    def incr(self, by):
        self._incr = Clause('INCREMENT BY', self._incr)
        return self

    def minval(self, val):
        mnvc = '{}MINVALUE'.format('NO ' if not val else "")
        self._minval = Clause(mnvc, val if val else _empty)
        return self

    def maxval(self, val):
        mxvc = '{}MAXVALUE'.format(
            'NO ' if not val or val == sys.maxsize else "")
        val = val if val and val != sys.maxsize else _empty
        self._maxval = Clause(mxvc, val)
        return self

    def start(self, val):
        self._start = Clause('START WITH', val)
        return self

    def cache(self, val):
        self._cache = Clause('CACHE', val)
        return self

    def cycle(sel):
        self._cycle = Clause('CYCLE')
        return self

    def owned_by(self, owner=None):
        if owner is None:
            safe('NONE')
        else:
            owner = self._cast_safe(owner)
        self._owned_by = Clause('OWNED BY', owner)
        return self

    def temporary(self):
        self._temporary = Clause('TEMP')
        return self

    def not_exists(self):
        self._not_exists = Clause('IF NOT EXISTS')
        return self

    @property
    def query(self):
        '''
        CREATE [ TEMPORARY | TEMP ] SEQUENCE name [ INCREMENT [ BY ]
                 increment ]
            [ MINVALUE minvalue | NO MINVALUE ]
            [ MAXVALUE maxvalue | NO MAXVALUE ]
            [ START [ WITH ] start ] [ CACHE cache ] [ [ NO ] CYCLE ]
            [ OWNED BY { table.column | NONE } ]
        '''
        self.orm.reset()
        typename = "{}SEQUENCE".format(
            self._temporary if self._temporary else "")
        self._add(Clause('CREATE {}'.format(typename), self.name),
                  self._incr,
                  self._minval,
                  self._maxval,
                  self._start,
                  self._cache,
                  self._cycle,
                  self._owned_by)
        return Raw(self.orm)
