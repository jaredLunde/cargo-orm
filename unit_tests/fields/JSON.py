#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import decimal
from vital.debug import RandData

from cargo.fields import Json
from cargo.fields.keyvalue import _jsontype

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestJson(configure.KeyValueTestCase, TestField):

    @property
    def base(self):
        return self.orm.json_field

    def test_init_(self):
        base = Json()
        rd = RandData(str).dict(4, 2)
        base(rd)
        self.assertIsInstance(base.value, dict)
        base.clear()

    def test___call__(self):
        base = Json()
        base(RandData(str).dict(4, 2))
        self.assertIsInstance(base.value, dict)
        self.assertIsInstance(base.value, _jsontype)
        base(RandData(str).list(4))
        self.assertIsInstance(base.value, list)
        self.assertIsInstance(base.value, _jsontype)
        base(RandData().randstr)
        self.assertIsInstance(base.value, str)
        self.assertIsInstance(base.value, _jsontype)
        base(RandData().randint)
        self.assertIsInstance(base.value, int)
        self.assertIsInstance(base.value, _jsontype)
        base(RandData().randfloat)
        self.assertIsInstance(base.value, float)
        self.assertIsInstance(base.value, _jsontype)
        base(decimal.Decimal(RandData().randfloat))
        self.assertIsInstance(base.value, decimal.Decimal)
        self.assertIsInstance(base.value, _jsontype)

    def test_insert(self):
        for val in (RandData(str).dict(4, 2), RandData(str).list(4),
                    RandData().randstr, RandData().randint):
            self.base(val)
            val = getattr(self.orm.new().insert(self.base),
                          self.base.field_name)
            self.assertEqual(val, val.value)
        for val in (RandData().randfloat,):
            self.base(val)
            val = getattr(self.orm.new().insert(self.base),
                          self.base.field_name)
            self.assertAlmostEqual(val, val.value)

    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base(RandData(str).dict(4, 2))
        self.orm.insert(self.base)
        val = getattr(self.orm.new().desc(self.orm.uid).get(),
                      self.base.field_name)
        self.assertDictEqual(val.value, self.base.value)

    def test_array_insert(self):
        arr = [RandData(str).dict(4, 2), RandData(str).dict(4, 2)]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, arr)

    def test_array_select(self):
        arr = [RandData(str).dict(4, 2), RandData(str).dict(4, 2)]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'json')
        self.assertEqual(self.base_array.type_name, 'json[]')


class TestEncJson(TestJson):

    @property
    def base(self):
        return self.orm.enc_json

    def test_init(self):
        pass


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestJson, TestEncJson, failfast=True, verbosity=2)
