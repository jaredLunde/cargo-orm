#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from kola import config

from vital.docr import Docr
from bloom.fields import Varchar
from bloom import create_pool

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Char import *


class TestVarchar(TestChar):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Varchar()
        self.base.table = 'test'
        self.base.field_name = 'char'

    def test_additional_kwargs(self):
        char = Varchar(minlen=1)
        self.assertEqual(char.minlen, 1)
        char = Varchar(maxlen=5)
        self.assertEqual(char.maxlen, 5)


if __name__ == '__main__':
    # Unit test
    unittest.main()
