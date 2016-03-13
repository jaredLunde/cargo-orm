#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Char

from unit_tests.fields.Field import TestField, Tc
from unit_tests import configure


class TestChar(configure.CharTestCase, TestField):

    @property
    def base(self):
        return self.orm.char

    def test_init(self, *args, **kwargs):
        base = self.base.__class__(5)
        self.assertEqual(base.value, base.empty)
        base = self.base.__class__(5, primary=True)
        self.assertEqual(base.primary, True)
        base = self.base.__class__(5, unique=True)
        self.assertEqual(base.unique, True)
        base = self.base.__class__(5, index=True)
        self.assertEqual(base.index, True)
        base = self.base.__class__(5, default='field')
        self.assertEqual(base.default, 'field')
        base = self.base.__class__(5, not_null=True)
        self.assertEqual(base.not_null, True)
        base = self.base.__class__(5, validator=Tc)
        self.assertIsInstance(base.validator, Tc)

    def test_validate(self):
        base = self.base.__class__(2)
        base.minlen, base.maxlen, base.not_null = 1, 2, True
        base('123')
        self.assertFalse(base.validate())
        base('12')
        self.assertTrue(base.validate())
        base('')
        self.assertFalse(base.validate())

    def test___call__(self):
        for val in (40, [], dict(), set(), tuple(), 'foo'):
            self.base(val)
            self.assertEqual(self.base.value, str(val))

    def test_additional_kwargs(self):
        char = self.base.__class__(1, minlen=1)
        self.assertEqual(char.minlen, 1)
        char = self.base.__class__(maxlen=5)
        self.assertEqual(char.maxlen, 5)

    def test_insert(self):
        self.base('foo')
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertNotEqual(val, 'foo')
        self.assertEqual(val.strip(), 'foo')

    def test_select(self):
        self.base('foo')
        self.orm.insert(self.base)
        r = getattr(self.orm.new().desc(self.orm.uid).get(self.base),
                    self.base.field_name)
        self.assertEqual(len(r.value), 200)
        self.assertEqual(r.value.strip(), 'foo')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestChar, failfast=True, verbosity=2)
