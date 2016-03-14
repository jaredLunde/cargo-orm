#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import re
import string

from bloom.fields import Email
from vital.debug import RandData, gen_rand_str

from unit_tests.fields.Char import *
from unit_tests import configure


class TestEmail(configure.ExtrasTestCase, TestChar):

    @property
    def base(self):
        return self.orm.email

    def test_validate(self):
        for email in RandData(RandData.emailType).list(200):
            self.base(email)
            self.assertTrue(self.base.validate())

        def rand():
            return gen_rand_str(
                3, 15, keyspace=string.ascii_letters + string.digits + '@')

        for _ in range(200):
            self.base(rand())
            self.assertFalse(self.base.validate())

    def test_insert(self):
        self.base('jared@gmail.com')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, 'jared@gmail.com')
        self.base('JaredLunde@gmail.com')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, 'jaredlunde@gmail.com')


    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base('jared@gmail.com')
        self.orm.insert(self.base)
        orm = self.orm.new().desc(self.orm.uid)
        self.assertEqual(getattr(orm.get(), self.base.field_name).value,
                         self.base.value)

    def test_array_insert(self):
        arr = ['jared@gmail.com', 'jaredlunde@gmail.com']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, arr)

    def test_array_select(self):
        arr = ['jared@gmail.com', 'jaredlunde@gmail.com']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


class TestEncEmail(TestEmail, TestEncChar):

    @property
    def base(self):
        return self.orm.enc_email

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')

    def test_deepcopy(self):
        pass


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestEmail, TestEncEmail, failfast=True, verbosity=2)
