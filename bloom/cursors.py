#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Postgres Cursors`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   Copyright (C) 2003-2010 Federico Di Gregorio  <fog@debian.org>
   Edited by Jared Lunde in 2016

"""
try:
    from cnamedtuple import namedtuple as nt
except ImportError:
    from collections import namedtuple as nt

from psycopg2.extensions import cursor as _cursor


__all__ = ('CNamedTupleCursor',)


class CNamedTupleCursor(_cursor):
    """ Copyright (C) 2003-2010 Federico Di Gregorio  <fog@debian.org> """
    Record = None

    def execute(self, query, vars=None):
        self.Record = None
        return super().execute(query, vars)

    def executemany(self, query, vars):
        self.Record = None
        return super().executemany(query, vars)

    def callproc(self, procname, vars=None):
        self.Record = None
        return super().callproc(procname, vars)

    def fetchone(self):
        t = super().fetchone()
        if t is not None:
            nt = self.Record
            if nt is None:
                nt = self.Record = self._make_nt()
            return nt._make(t)

    def fetchmany(self, size=None):
        ts = super().fetchmany(size)
        nt = self.Record
        if nt is None:
            nt = self.Record = self._make_nt()
        return list(map(nt._make, ts))

    def fetchall(self):
        ts = super().fetchall()
        nt = self.Record
        if nt is None:
            nt = self.Record = self._make_nt()
        return list(map(nt._make, ts))

    def __iter__(self):
        it = super().__iter__()
        t = next(it)

        nt = self.Record
        if nt is None:
            nt = self.Record = self._make_nt()

        yield nt._make(t)

        while 1:
            yield nt._make(next(it))

    def _make_nt(self):
        return nt("Record", (d[0] for d in self.description or []),
                  rename=True)
