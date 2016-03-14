#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Varchar

from unit_tests.fields.Char import TestChar, TestEncChar
from unit_tests import configure


class TestVarchar(TestChar):

    @property
    def base(self):
        return self.orm.varchar

    def test_select(self):
        self.base('foo')
        self.orm.insert(self.base)
        r = getattr(self.orm.new().desc(self.orm.uid).get(self.base),
                    self.base.field_name)
        self.assertEqual(r.value, 'foo')

    def test_insert(self):
        self.base('foo')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, 'foo')

    def test_select(self):
        self.base('foo')
        self.orm.insert(self.base)
        r = getattr(self.orm.new().desc(self.orm.uid).get(self.base),
                    self.base.field_name)
        self.assertEqual(r.value, 'foo')

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'varchar(200)')
        self.assertEqual(self.base_array.type_name, 'varchar(200)[]')


class TestEncVarchar(TestVarchar, TestEncChar):
    @property
    def base(self):
        return self.orm.enc_varchar

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestVarchar, TestEncVarchar, failfast=True, verbosity=2)
