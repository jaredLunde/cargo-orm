#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from bloom.fields import Text

from unit_tests.fields.Char import *


class TestText(TestChar):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Text()
        self.base.table = 'test'
        self.base.field_name = 'text'


if __name__ == '__main__':
    # Unit test
    unittest.main()
