#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.builder.create_user`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import random

from bloom import fields
from bloom.builder import create_cast
from bloom.builder.fields import Column, find_column

from vital.debug import line

from unit_tests import configure


class TestColumn(unittest.TestCase):

    def test_init(self):
        for field in dir(fields):
            field = getattr(fields, field)
            if hasattr(field, 'OID') and field is not fields.Field:
                line('-')
                print(field.__name__, '|', field.OID)
                try:
                    field_ = field()
                    field_.field_name = 'foo'
                    c = find_column(field_)
                    print(c)
                except TypeError:
                    pass
            if hasattr(field, 'maxlen'):
                try:
                    field_ = field(maxlen=250)
                    field_.field_name = 'foo'
                    c = find_column(field_)
                    print(c)
                except TypeError:
                    pass
            if hasattr(field, 'minlen'):
                try:
                    field_ = field(maxlen=250, minlen=3)
                    field_.field_name = 'foo'
                    c = find_column(field_)
                    print(c)
                except TypeError:
                    pass
            if hasattr(field, 'minval'):
                try:
                    field_ = field(minval=14)
                    field_.field_name = 'foo'
                    c = find_column(field_)
                    print(c)
                except TypeError:
                    pass
            if hasattr(field, 'maxval'):
                try:
                    field_ = field(minval=14, maxval=32)
                    field_.field_name = 'foo'
                    c = find_column(field_)
                    print(c)
                except TypeError:
                    pass
            if hasattr(field, 'length'):
                try:
                    field_ = field(length=15)
                    field_.field_name = 'foo'
                    c = find_column(field_)
                    print(c)
                except TypeError:
                    pass
            if hasattr(field, 'types'):
                field_ = field(types=('foo', 'bar', 'baz'))
                field_.field_name = 'foo'
                c = find_column(field_)
                print(c)

    def test_bit(self):
        pass

    def test_varbit(self):
        pass

    def test_text(self):
        pass

    def test_numeric(self):
        for field in [fields.Decimal, fields.Currency] * 16:
            decimal_places = random.choice([-1, 0, 5])
            precision = random.choice([-1, 5, 14, 18])
            field_ = field(decimal_places=decimal_places, digits=precision)
            field_.field_name = 'foo'
            c = find_column(field_)
            print(c)

        for field in [fields.Float, fields.Double] * 10:
            precision = random.choice([-1, 5, 14, 18, 21])
            field_ = field(decimal_places=precision)
            field_.field_name = 'foo'
            c = find_column(field_)
            print(c)

    def test_int(self):
        pass

    def test_enum(self):
        pass

    def test_varchar(self):
        pass

    def test_char(self):
        pass

    def test_array(self):
        for field in dir(fields):
            field = getattr(fields, field)
            if hasattr(field, 'OID') and field is not fields.Field and \
               field is not fields.Array:
                try:
                    line('-')
                    print(field.__name__, 'Array |', field.OID)
                    field_ = fields.Array(field(),
                                          dimensions=random.randint(1, 3),
                                          maxlen=random.randint(-1, 2),
                                          minlen=random.randint(0, 4))
                    field_.field_name = 'foo'
                    c = find_column(field_)
                    print(c)
                except TypeError:
                    pass

    def test_encrypted(self):
        for field in dir(fields):
            field = getattr(fields, field)
            if hasattr(field, 'OID') and field is not fields.Field and \
               field is not fields.Encrypted:
                try:
                    line('-')
                    print(field.__name__, 'Encrypted |', field.OID)
                    field_ = fields.Encrypted(fields.Encrypted.generate_secret(),
                                              field())
                    field_.field_name = 'foo'
                    c = find_column(field_)
                    print(c)
                except TypeError:
                    pass

    def test_encrypted_array(self):
        pass


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestColumn, failfast=True, verbosity=2)
