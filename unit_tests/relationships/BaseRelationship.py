#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for vital.sql.relationships.BaseRelationship`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from vital import config

from vital.sql.relationships import BaseRelationship


class TestBaseRelationship(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.base = BaseRelationship()
        except:
            pass

    def test__copy(self):
        pass

    def test_set_join_alias(self):
        pass

    def test___init__(self):
        pass


if __name__ == '__main__':
    # Unit test
    unittest.main()
