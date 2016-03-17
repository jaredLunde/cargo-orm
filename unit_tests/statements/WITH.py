#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.statements.With`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
from copy import copy

from cargo import *
from cargo.statements import With

from unit_tests import configure
from unit_tests.configure import new_clause, new_field, new_expression


class TestWith(configure.StatementTestCase):

    def test_with_union(self):
        t = safe('t')
        n = safe('n')
        orm = ORM().use(t)
        orm.state.add_fields(n+1)
        with (
          Raw(ORM().values(1), alias=t, recursive=(n,)) +
          Select(orm)
        ) as orm:
            orm.use(t).limit(10).select(n)
        self.assertEqual(len(orm.result), 10)

        orm = ORM().use(t)
        orm.state.add_fields(n+1)
        q = With(
            orm,
            Raw(ORM().values(1), alias=t, recursive=(n,)) +
            Select(orm))
        q.orm.use(t).limit(10).select(n)
        self.assertEqual(len(q.execute().fetchall()), 10)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestWith, failfast=True, verbosity=2)
