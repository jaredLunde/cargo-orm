#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import decimal
from vital.debug import RandData

from bloom.fields import Json
from bloom.fields.keyvalue import _jsontype

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
                    RandData().randstr, RandData().randint,
                    RandData().randfloat):
            self.base(val)
            val = getattr(self.orm.naked().insert(self.base),
                          self.base.field_name)
            self.assertAlmostEqual(self.base.value, val)

    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base(RandData(str).dict(4, 2))
        self.orm.insert(self.base)
        val = getattr(self.orm.naked().desc(self.orm.uid).get(),
                      self.base.field_name)
        self.assertEqual(val, self.base.value)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestJson, failfast=True, verbosity=2)
