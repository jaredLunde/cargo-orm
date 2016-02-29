#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest
import string

from bloom.fields import Email
from vital.debug import RandData, gen_rand_str

from unit_tests.fields.Field import *


class TestEmail(TestField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Email()
        self.base.table = 'test'
        self.base.field_name = 'email'

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



if __name__ == '__main__':
    # Unit test
    unittest.main()
