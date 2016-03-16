#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from cargo.fields import Text

from unit_tests.fields.Varchar import TestVarchar, TestEncVarchar
from unit_tests import configure


class TestText(TestVarchar):

    @property
    def base(self):
        return self.orm.text

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


class TestEncText(TestText, TestEncVarchar):
    @property
    def base(self):
        return self.orm.enc_text


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestText, TestEncText, failfast=True, verbosity=2)
