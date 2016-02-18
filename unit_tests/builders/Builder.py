#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for vital.sql.build.TableMeta`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from vital import config
from vital.sql.build import TableMeta


cfile = '/home/jared/apps/xfaps/vital.json'


class TestTableMeta(unittest.TestCase):
    pass


if __name__ == '__main__':
    # Unit test
    unittest.main()
