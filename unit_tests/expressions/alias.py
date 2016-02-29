#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Unit tests for bloom.expressions.alias`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde

"""
import unittest

from vital.debug import RandData
from bloom.expressions import aliased


class Testaliased(unittest.TestCase):

    def test___init__(self):
        for v in RandData(str).list(10):
            self.base = aliased(v)
            self.assertEqual(self.base.string, v)
            self.assertEqual(str(self.base), v)
            self.assertEqual(
                self.base.alias('foo_alias'),
                "{} AS foo_alias".format(str(self.base)))


if __name__ == '__main__':
    # Unit test
    unittest.main()
