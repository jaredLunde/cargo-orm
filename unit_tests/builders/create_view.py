#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.builder.TableMeta`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from kola import config
from bloom import ORM, db, create_kola_db
from bloom.builder import create_view


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_db()


cfile = '/home/jared/apps/xfaps/vital.json'


class TestCreateEnumType(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        print(db.select(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        print(create_view(
            self.orm,
            'fisher',
            self.orm.dry().select(1), 'fish', 'fosh', 'nosh',
            temporary=True,
            run=False))
        print(create_view(
            self.orm,
            'fisher',
            self.orm.dry().select(1), 'fish', 'fosh', 'nosh',
            temporary=True,
            materialized=True,
            security_barrier=True,
            run=False))


if __name__ == '__main__':
    # Unit test
    unittest.main()
