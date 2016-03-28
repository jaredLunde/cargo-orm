"""

  `Postgres Cursors`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   Copyright (C) 2003-2010 Federico Di Gregorio  <fog@debian.org>
   Edited by Jared Lunde in 2016

"""
import copy
from collections import OrderedDict

try:
    from cnamedtuple import namedtuple as nt
except ImportError:
    from collections import namedtuple as nt

from psycopg2.extensions import cursor as _cursor
from vital.cache import cached_property


__all__ = (
    'CNamedTupleCursor',
    'OrderedDictCursor',
    'ModelCursor'
)


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
        return nt("Record", (d for d, *_ in self.description or []),
                  rename=True)


class OrderedDictCursor(_cursor):

    def _to_od(self, tup):
        return OrderedDict((k[0], v) for k, v in zip(self.description, tup))

    def execute(self, query, vars=None):
        return super().execute(query, vars)

    def executemany(self, query, vars):
        return super().executemany(query, vars)

    def callproc(self, procname, vars=None):
        return super().callproc(procname, vars)

    def fetchone(self):
        t = super().fetchone()
        if t is not None:
            return self._to_od(t)

    def fetchmany(self, size=None):
        ts = super().fetchmany(size)
        return list(map(self._to_od, ts))

    def fetchall(self):
        ts = super().fetchall()
        return list(map(self._to_od, ts))

    def __iter__(self):
        it = super().__iter__()
        t = next(it)
        yield self._to_od(t)
        while 1:
            yield self._to_od(next(it))


class ModelCursor(_cursor):

    def _fill_model(self, tup, new=True):
        model = self._cargo_model.clear_copy() if new else self._cargo_model
        for (k, *_), v in zip(self.description, tup):
            model[k] = v
        return model

    def execute(self, query, vars=None):
        return super().execute(query, vars)

    def executemany(self, query, vars):
        return super().executemany(query, vars)

    def callproc(self, procname, vars=None):
        return super().callproc(procname, vars)

    def fetchone(self):
        t = super().fetchone()
        if t is not None:
            return self._fill_model(t, new=self._cargo_model._new)

    def fetchmany(self, size=None):
        ts = super().fetchmany(size)
        return list(map(self._fill_model, ts))

    def fetchall(self):
        ts = super().fetchall()
        return list(map(self._fill_model, ts))

    def __iter__(self):
        it = super().__iter__()
        t = next(it)
        yield self._fill_model(t)
        while 1:
            yield self._fill_model(next(it))
