#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for bloom.validators.ValidationValue`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from kola import config
from vital.security import randkey

from bloom import *
from bloom.validators import ValidationValue

from bloom.orm import QueryState
from bloom.statements import WITH


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_pool()


def new_field(type='char', value=None, name=None, table=None):
    field = getattr(fields, type.title())(value=value)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestValidationValue(unittest.TestCase):

    def test_int(self):
        field = new_field('int', value=2)
        base = ValidationValue(field)
        self.assertTrue(base.value is field.value)
        self.assertTrue(base.is_int)
        self.assertFalse(base.is_float)
        self.assertFalse(base.is_str)
        self.assertFalse(base.is_array)
        self.assertTrue(base.len == 1)

    def test_str(self):
        field = new_field('char', value='2.40')
        base = ValidationValue(field)
        self.assertTrue(base.value is field.value)
        self.assertTrue(base.is_str)
        self.assertFalse(base.is_float)
        self.assertFalse(base.is_int)
        self.assertFalse(base.is_array)
        self.assertTrue(base.len == 4)

    def test_array(self):
        field = new_field('array', value=[2.40])
        base = ValidationValue(field)
        self.assertTrue(base.value is field.value)
        self.assertTrue(base.is_array)
        self.assertFalse(base.is_float)
        self.assertFalse(base.is_str)
        self.assertFalse(base.is_int)
        self.assertTrue(base.len == 1)

    def test_float(self):
        field = new_field('float', value=2.41)
        base = ValidationValue(field)
        self.assertTrue(base.value is field.value)
        self.assertTrue(base.is_float)
        self.assertFalse(base.is_int)
        self.assertFalse(base.is_str)
        self.assertFalse(base.is_array)
        self.assertTrue(base.len == 4)


if __name__ == '__main__':
    # Unit test
    unittest.main()
