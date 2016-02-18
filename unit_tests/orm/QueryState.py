#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for vital.sql.orm.QueryState`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from vital import config

from vital.sql.orm import QueryState


class TestQueryState(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.base = QueryState()
        except:
            pass

    def test_add_clause(self):
        pass

    def test_has_clause(self):
        pass

    def test___init__(self):
        pass


if __name__ == '__main__':
    # Unit test
    unittest.main()
