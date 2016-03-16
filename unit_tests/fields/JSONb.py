#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import decimal
from vital.debug import RandData

from cargo.fields import JsonB
from cargo.fields.keyvalue import _jsontype

from unit_tests.fields.JSON import TestJson
from unit_tests import configure


class TestJsonB(TestJson):

    @property
    def base(self):
        return self.orm.jsonb_field

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'jsonb')
        self.assertEqual(self.base_array.type_name, 'jsonb[]')


class TestEncJsonB(TestJsonB):

    @property
    def base(self):
        return self.orm.enc_jsonb

    def test_init(self):
        pass


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestJsonB, TestEncJsonB, failfast=True, verbosity=2)
