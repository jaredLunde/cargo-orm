#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for vital.sql.expressions.ArrayItems`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest

from vital.debug import RandData
from vital.sql.expressions import ArrayItems


class TestArrayItems(unittest.TestCase):

    def test___init__(self):
        for x in RandData(list).list(10):
            self.base = ArrayItems(x)
            self.assertListEqual(self.base.value, x)


if __name__ == '__main__':
    # Unit test
    unittest.main()
