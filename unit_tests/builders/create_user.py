#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.builder.create_user`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest

from cargo import ORM, db
from cargo.builder import create_user, create_role


class TestCreateRole(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        print(db.select(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        u = create_user(self.orm, 'foo', 'login', 'superuser', 'createrole',
                        'createuser', 'createdb', dry=True)
        u.password('fishtaco')
        u.in_role('cream', 'based', 'sauce')
        u.connlimit(1000)
        print(u.query.mogrified)


if __name__ == '__main__':
    # Unit test
    unittest.main()
