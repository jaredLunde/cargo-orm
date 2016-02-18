#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import re
import unittest
import string

from bloom.etc.usernames import reserved_usernames
from bloom.fields import Username
from vital.debug import RandData, gen_rand_str

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.fields.Char import *


class TestUsername(TestChar):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Username()
        self.base.table = 'test'
        self.base.field_name = 'username'

    def test_validate(self):
        self.base = Username()
        for username in reserved_usernames:
            self.base(username)
            self.assertFalse(self.base.validate())

        randlist = RandData(str).list(1000)
        self.base = Username(reserved_usernames=randlist)
        for username in randlist:
            self.base(username)
            self.assertFalse(self.base.validate())

        self.base = Username(reserved_usernames=[])
        for username in randlist:
            self.base(username)
            self.assertTrue(self.base.validate())

    def test_pattern(self):
        ure = re.compile(r"""[a-z]""")
        self.base = Username(re_pattern=ure)
        for _ in range(200):
            username = gen_rand_str(keyspace='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            self.base(username)
            self.assertFalse(self.base.validate())

        ure = re.compile(r"""[a-z]""")
        self.base = Username(re_pattern=ure)
        for _ in range(200):
            username = gen_rand_str(keyspace='abcdefghijklmnopqrstuvwxyz')
            self.base(username)
            self.assertTrue(self.base.validate())

    def test_add_reserved_username(self):
        self.base = Username(reserved_usernames=[])
        randlist = RandData(str).list(1000)
        self.base.add_reserved_username(*randlist)
        for username in randlist:
            self.base(username)
            self.assertFalse(self.base.validate())


if __name__ == '__main__':
    # Unit test
    unittest.main()
