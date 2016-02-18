#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for bloom.orm.RestModel`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from kola import config

from bloom.orm import RestModel


class TestRestModel(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.base = RestModel()
        except:
            pass

    def test_post(self):
        pass

    def test_put(self):
        pass

    def test_patch(self):
        pass

    def test_get(self):
        pass

    def test_delete(self):
        pass


if __name__ == '__main__':
    # Unit test
    unittest.main()
