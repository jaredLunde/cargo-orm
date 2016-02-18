#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.exceptions.ValidationError`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from kola import config

from bloom.exceptions import ValidationError


class TestValidationError(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.base = ValidationError()
        except:
            pass

    def test___init__(self):
        pass


if __name__ == '__main__':
    # Unit test
    unittest.main()
