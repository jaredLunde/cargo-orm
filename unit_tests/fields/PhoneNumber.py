#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import phonenumbers

from unit_tests.fields.Field import *
from unit_tests import configure


class TestPhoneNumber(configure.ExtrasTestCase, TestField):

    @property
    def base(self):
        return self.orm.phone

    def test_init(self, *args, **kwargs):
        base = self.base.__class__()
        self.assertEqual(base.value, base.empty)
        base = self.base.__class__(primary=True)
        self.assertEqual(base.primary, True)
        base = self.base.__class__(unique=True)
        self.assertEqual(base.unique, True)
        base = self.base.__class__(index=True)
        self.assertEqual(base.index, True)
        base = self.base.__class__(default='field')
        self.assertEqual(base.default, 'field')
        base = self.base.__class__(not_null=True)
        self.assertEqual(base.not_null, True)
        base = self.base.__class__(validator=Tc)
        self.assertIsInstance(base.validator, Tc)
        base = self.base.__class__(region='GB')
        self.assertEqual(base.region, 'GB')

    def test_validate(self):
        for phone in ('+44 3069 990403', '3069 990403', '9203441014',
                      '8878315'):
            self.base(phone)
            self.assertTrue(self.base.validate())
        for phone in ('123', '1234', '12', '12345'):
            self.base(phone)
            self.assertFalse(self.base.validate())

    def test___call__(self):
        with self.assertRaises(Exception):
            self.base('1')
        with self.assertRaises(Exception):
            self.base(1)
        self.base(6081153379)
        self.assertIsInstance(self.base.value, phonenumbers.PhoneNumber)
        self.assertEqual(self.base.value.national_number, 6081153379)
        self.base('6081153380')
        self.assertEqual(self.base.value.national_number, 6081153380)

    def test_from_text(self):
        self.base.from_text('Call me at 9203441457x230')
        self.assertEqual(self.base.value.national_number, 9203441457)
        self.assertEqual(self.base.value.extension, '230')

    def test_insert(self):
        self.base('+44 3069 990403')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, self.base.value)
        self.base('+16084453020x302')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, self.base.value)

    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base('+16084453020x302')
        self.orm.insert(self.base)
        orm = self.orm.new().desc(self.orm.uid)
        self.assertEqual(getattr(orm.get(), self.base.field_name).value,
                         self.base.value)

    def test_array_insert(self):
        arr = ['+16084453020x302', '+44 3069 990403']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, self.base_array.value)

    def test_array_select(self):
        arr = ['+16084453020x302', '+44 3069 990403']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


class TestEncPhoneNumber(TestPhoneNumber):

    @property
    def base(self):
        return self.orm.enc_phone

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestPhoneNumber,
                        TestEncPhoneNumber,
                        failfast=True,
                        verbosity=2)
