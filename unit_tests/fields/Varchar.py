#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Varchar

from unit_tests.fields.Char import TestChar
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
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertEqual(val, 'foo')

    def test_select(self):
        self.base('foo')
        self.orm.insert(self.base)
        r = getattr(self.orm.new().desc(self.orm.uid).get(self.base),
                    self.base.field_name)
        self.assertEqual(r.value, 'foo')

        
if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestVarchar, failfast=True, verbosity=2)
