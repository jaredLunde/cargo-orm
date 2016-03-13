#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import decimal
from vital.debug import RandData

from bloom.fields import JsonB
from bloom.fields.keyvalue import _jsontype

from unit_tests.fields.JSON import TestJson
from unit_tests import configure


class TestJsonB(TestJson):

    @property
    def base(self):
        return self.orm.jsonb_field


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestJsonB, failfast=True, verbosity=2)
