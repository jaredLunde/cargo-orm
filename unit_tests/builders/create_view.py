#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.builder.create_view`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from kola import config
from cargo import ORM, db, create_kola_db
from cargo.builder import create_view


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


class TestCreateView(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        print(db.select(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        print(create_view(
            self.orm,
            'fisher',
            'fish', 'fosh', 'nosh',
            query=self.orm.dry().select(1),
            temporary=True,
            dry=True))
        print(create_view(
            self.orm,
            'fisher',
            'fish', 'fosh', 'nosh',
            query=self.orm.dry().select(1),
            temporary=True,
            materialized=True,
            security_barrier=True,
            dry=True))


if __name__ == '__main__':
    # Unit test
    unittest.main()
