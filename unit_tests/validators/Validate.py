#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
  `Unit tests for bloom.validators.Validate`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import random
import unittest
from collections import *

from kola import config
from vital.security import randkey
from vital.docr import Docr
from vital.debug import gen_rand_str, RandData

from bloom import *
from bloom.validators import Validate, ValidationValue

from bloom.orm import QueryState
from bloom.statements import WITH


config.bind('/home/jared/apps/xfaps/vital.json')
create_kola_pool()


def new_field(type='char', value=None, name=None, table=None, **kwargs):
    type = type.title() if not type[0].isupper() else type
    field = getattr(fields, type)(value=value, **kwargs)
    field.field_name = name or randkey(24)
    field.table = table or randkey(24)
    return field


class TestValidate(unittest.TestCase):
    docr = Docr(fields)

    def _get_field_data(self, field, charsize=0, numrange=None, arrsize=None):
        if field.sqltype in Validate.CHARS:
            if not charsize:
                return 'foo'
            else:
                return gen_rand_str(charsize)
        elif field.sqltype in Validate.INTS:
            if not intrange:
                return 1
            else:
                return random.randint(*numrange)
        elif field.sqltype in Validate.FLOATS:
            if not intrange:
                return 1
            else:
                return random.uniform(*numrange)
        elif field.sqltype in Validate.ARRAYS:
            if not arrsize:
                return [1,]
            else:
                return RandData(int).list(arrsize)

    def test___init__(self):
        field = new_field('char', value='foo')
        v = Validate(field)
        self.assertIs(v.field, field)
        self.assertIsInstance(v.value, ValidationValue)
        self.assertIs(v.error, None)

    def test__validate_not_null(self):
        for n, f in self.docr.classes.items():
            if isinstance(f, Field):
                field = new_field(n, not_null=True)
                v = Validate(field)
                self.assertFalse(v._validate_not_null())
                field.value = self._get_field_data(field)
                v = Validate(field)
                self.assertTrue(v._validate_not_null())

    def test__is_valid_null(self):
        for n, f in self.docr.classes.items():
            if isinstance(f, Field):
                field = new_field(n, not_null=False)
                v = Validate(field)
                self.assertTrue(v._is_valid_null())

    def test_maxlen_str(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and \
               f.obj.sqltype in Validate.CHARS and f.obj != Slug:
                #: Fail
                field = new_field(n, maxlen=15)
                field(gen_rand_str(16))
                v = Validate(field)
                self.assertFalse(v.maxlen())
                #: Pass
                field(gen_rand_str(15))
                v = Validate(field)
                self.assertTrue(v.maxlen())
                #: Pass always
                field = new_field(n, maxlen=None)
                field(gen_rand_str(150))
                v = Validate(field)
                self.assertTrue(v.maxlen())

                v = Validate(field)
                field = new_field(n, maxlen=-1)
                field(gen_rand_str(150))
                v = Validate(field)
                self.assertTrue(v.maxlen())


    def test_minlen_str(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and f.obj.sqltype in Validate.CHARS:
                #: Pass
                field = new_field(n, minlen=15)
                field(gen_rand_str(15))
                v = Validate(field)
                self.assertTrue(v.minlen())
                #: Fail
                field(gen_rand_str(14))
                v = Validate(field)
                self.assertFalse(v.minlen())
                #: Pass always
                field = new_field(n, minlen=None)
                field(gen_rand_str(150))
                v = Validate(field)
                self.assertTrue(v.minlen())

                field = new_field(n, minlen=0)
                field(gen_rand_str(150))
                v = Validate(field)
                self.assertTrue(v.minlen())

    def test_str(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and f.obj.sqltype in Validate.CHARS:
                field = new_field(n)
                field(gen_rand_str(25))
                v = Validate(field)
                self.assertTrue(v.str())
                field(random.randint(10, 10))
                v = Validate(field)
                self.assertTrue(v.str())
                field.validation_value = 13
                v = Validate(field)
                self.assertFalse(v.str())

    def test__validate_chars(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and f.obj.sqltype in Validate.CHARS\
            and f.obj != Slug:
                field = new_field(n, minlen=15, maxlen=25)
                #: Pass
                field(gen_rand_str(15))
                v = Validate(field)
                self.assertTrue(v._validate_chars())
                field(gen_rand_str(25))
                v = Validate(field)
                self.assertTrue(v._validate_chars())
                #: Fail
                field(gen_rand_str(14))
                v = Validate(field)
                self.assertFalse(v._validate_chars())
                field(gen_rand_str(26))
                v = Validate(field)
                self.assertFalse(v._validate_chars())

    def test_maxlen_array(self):
        n = 'array'
        #: Fail
        field = new_field(n, maxlen=15)
        field(RandData(str).list(16))
        v = Validate(field)
        self.assertFalse(v.maxlen())
        #: Pass
        field(RandData(str).list(15))
        v = Validate(field)
        self.assertTrue(v.maxlen())
        #: Pass always
        field = new_field(n, maxlen=None)
        field(RandData(str).list(100))
        v = Validate(field)
        self.assertTrue(v.maxlen())

        v = Validate(field)
        field = new_field(n, maxlen=-1)
        field(RandData(str).list(100))
        v = Validate(field)
        self.assertTrue(v.maxlen())

    def test_minlen_array(self):
        n = 'array'
        #: Pass
        field = new_field(n, minlen=15)
        field(RandData(str).list(15))
        v = Validate(field)
        self.assertTrue(v.minlen())
        #: Fail
        field(RandData(str).list(14))
        v = Validate(field)
        self.assertFalse(v.minlen())
        #: Pass always
        field = new_field(n, minlen=None)
        field(RandData(str).list(100))
        v = Validate(field)
        self.assertTrue(v.minlen())

        field = new_field(n, minlen=0)
        field(RandData(str).list(100))
        v = Validate(field)
        self.assertTrue(v.minlen())

    def test_array(self):
        field = new_field('array')
        for t in (set, list, tuple, deque, UserList):
            field.value = t(RandData(str).list(10))
            v = Validate(field)
            self.assertTrue(v.array())

    def test__validate_arrays(self):
        n = 'array'
        field = new_field(n, minlen=15, maxlen=25)
        #: Pass
        field(RandData(str).list(15))
        v = Validate(field)
        self.assertTrue(v._validate_arrays())
        field(RandData(str).list(25))
        v = Validate(field)
        self.assertTrue(v._validate_arrays())
        #: Fail
        field(RandData(str).list(14))
        v = Validate(field)
        self.assertFalse(v._validate_arrays())
        field(RandData(str).list(26))
        v = Validate(field)
        self.assertFalse(v._validate_arrays())

    def test_minval(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and \
               f.obj.sqltype in (Validate.FLOATS | Validate.INTS):
                field = new_field(n, minval=5)
                # Pass
                field(5)
                v = Validate(field)
                self.assertTrue(v.minval())
                # Fail
                field(4)
                v = Validate(field)
                self.assertFalse(v.minval())
                # Pass always
                field = new_field(n, minval=-1)
                field(4)
                v = Validate(field)
                self.assertTrue(v.minval())

                field = new_field(n, minval=None)
                field(4)
                v = Validate(field)
                self.assertTrue(v.minval())

    def test_maxval(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and \
               f.obj.sqltype in (Validate.FLOATS | Validate.INTS):
                field = new_field(n, maxval=5)
                # Pass
                field(5)
                v = Validate(field)
                self.assertTrue(v.maxval())
                # Fail
                field(6)
                v = Validate(field)
                self.assertFalse(v.maxval())
                field = new_field(n, maxval=-1)
                field(400)
                v = Validate(field)
                self.assertFalse(v.maxval())

                # Pass always
                field = new_field(n, maxval=None)
                field(400)
                v = Validate(field)
                self.assertTrue(v.maxval())

    def test_int(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and f.obj.sqltype in Validate.INTS:
                field = new_field(n)
                # Pass
                field.value = 5
                v = Validate(field)
                self.assertTrue(v.int())
                # Fail
                field.value = '5'
                v = Validate(field)
                self.assertFalse(v.int())
                for t in (set, list, tuple, deque, UserList):
                    field.value = t([5])
                    v = Validate(field)
                    self.assertFalse(v.int())


    def test__validate_ints(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and f.obj.sqltype in Validate.INTS:
                field = new_field(n, minval=15, maxval=25)
                #: Pass
                field(15)
                v = Validate(field)
                self.assertTrue(v._validate_ints())
                field(25)
                v = Validate(field)
                self.assertTrue(v._validate_ints())
                #: Fail
                field(14)
                v = Validate(field)
                self.assertFalse(v._validate_ints())
                field(26)
                v = Validate(field)
                self.assertFalse(v._validate_ints())
                field.value = 26.0
                v = Validate(field)
                self.assertFalse(v._validate_ints())
                for t in (set, list, tuple, deque, UserList):
                    field.value = t([5])
                    v = Validate(field)
                    self.assertFalse(v._validate_ints())

    def test_float(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and f.obj.sqltype in Validate.FLOATS:
                field = new_field(n)
                # Pass
                field.value = 5.0
                v = Validate(field)
                self.assertTrue(v.float())
                # Fail
                field.value = '5'
                v = Validate(field)
                self.assertFalse(v.float())
                field.value = 5
                v = Validate(field)
                self.assertFalse(v.float())
                for t in (set, list, tuple, deque, UserList):
                    field.value = t([5])
                    v = Validate(field)
                    self.assertFalse(v.float())

    def test__validate_floats(self):
        for n, f in self.docr.classes.items():
            if hasattr(f.obj, 'sqltype') and f.obj.sqltype in Validate.FLOATS:
                field = new_field(n, minval=15.0, maxval=25.0)
                #: Pass
                field(15.0)
                v = Validate(field)
                self.assertTrue(v._validate_floats())
                field(25.0)
                v = Validate(field)
                self.assertTrue(v._validate_floats())
                #: Fail
                field(14.0)
                v = Validate(field)
                self.assertFalse(v._validate_floats())
                field(26.0)
                v = Validate(field)
                self.assertFalse(v._validate_floats())
                field.value = 26
                v = Validate(field)
                self.assertFalse(v._validate_floats())
                for t in (set, list, tuple, deque, UserList):
                    field.value = t([5])
                    v = Validate(field)
                    self.assertFalse(v._validate_floats())


if __name__ == '__main__':
    # Unit test
    unittest.main()
