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
from bloom import ORM, create_kola_client
from bloom.builder import Builder


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)
create_kola_client()


class TestTableMeta(unittest.TestCase):
    orm = ORM()

    def test_builder(self):
        b = Builder(self.orm, 'xfaps')
        b.run()


if __name__ == '__main__':
    # Unit test
    unittest.main()
