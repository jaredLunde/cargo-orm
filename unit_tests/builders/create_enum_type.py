#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.builder.TableMeta`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from kola import config
from cargo import ORM, create_kola_client
from cargo.builder import create_enum_type


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_client()


cfile = '/home/jared/apps/xfaps/vital.json'


class TestCreateEnumType(unittest.TestCase):
    orm = ORM()

    def test_create(self):
        print(create_enum_type(
            self.orm, 'colors', 'red', 'white', 'blue', dry=True))


if __name__ == '__main__':
    # Unit test
    unittest.main()
