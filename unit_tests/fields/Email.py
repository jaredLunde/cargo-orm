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
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertEqual(val, 'jared@gmail.com')
        self.base('JaredLunde@gmail.com')
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertEqual(val, 'jaredlunde@gmail.com')


    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base('jared@gmail.com')
        self.orm.insert(self.base)
        self.assertEqual(self.orm.new().desc(self.orm.uid).get().email.value,
                         self.base.value)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestEmail, failfast=True, verbosity=2)
