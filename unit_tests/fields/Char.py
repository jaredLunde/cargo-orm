#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Char
from bloom.etc import types

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
        for val in (40, [], dict(), set(),  'foo'):
            self.base(val)
            self.assertEqual(self.base.value, str(val))

    def test_additional_kwargs(self):
        char = self.base.__class__(1, minlen=1)
        self.assertEqual(char.minlen, 1)
        char = self.base.__class__(maxlen=5)
        self.assertEqual(char.maxlen, 5)

    def test_insert(self):
        self.base('foo')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value.strip(), 'foo')

    def test_select(self):
        self.base('foo')
        self.orm.insert(self.base)
        r = getattr(self.orm.new().desc(self.orm.uid).get(self.base),
                    self.base.field_name)
        self.assertEqual(len(r.value), 200)
        self.assertEqual(r.value.strip(), 'foo')

    def test_array_insert(self):
        arr = ['bingo', 'bango', 'bongo']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name).value
        if self.base_array.OID == types.CHAR:
            self.assertNotEqual(val, arr)
        self.assertListEqual([v.strip() for v in val], arr)

    def test_array_select(self):
        arr = ['bingo', 'bango', 'bongo']
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'char(200)')
        self.assertEqual(self.base_array.type_name, 'char(200)[]')


class TestEncChar(TestChar):

    @property
    def base(self):
        return self.orm.enc_char

    def test_init(self, *args, **kwargs):
        pass

    def test_validate(self):
        pass

    def test_additional_kwargs(self):
        pass

    def test_insert(self):
        self.base('foo')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value.strip(), 'foo')

    def test_select(self):
        self.base('foo')
        self.orm.insert(self.base)
        r = getattr(self.orm.new().desc(self.orm.uid).get(self.base),
                    self.base.field_name)
        self.assertEqual(r.value.strip(), 'foo')

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestChar, TestEncChar, failfast=True, verbosity=2)
