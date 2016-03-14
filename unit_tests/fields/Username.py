#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import re
import string

from bloom.etc.usernames import reserved_usernames
from bloom.fields import Username
from vital.debug import RandData, gen_rand_str

from unit_tests.fields.Char import *
from unit_tests import configure


class TestUsername(configure.ExtrasTestCase, TestChar):

    @property
    def base(self):
        return self.orm.username

    def test_validate(self):
        base = Username()
        for username in reserved_usernames:
            base(username.upper())
            self.assertFalse(base.validate())

        randlist = set(s.lower() for s in RandData(str).list(1000))
        base = Username(reserved_usernames=randlist)
        for username in randlist:
            base(username.upper())
            self.assertFalse(base.validate())

        base = Username(reserved_usernames=[])
        for username in randlist:
            base(username)
            self.assertTrue(base.validate())

    def test_pattern(self):
        ure = re.compile(r"""[a-z]""")
        base = Username(re_pattern=ure)
        for _ in range(100):
            username = gen_rand_str(keyspace='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            base(username)
            self.assertFalse(base.validate())

        ure = re.compile(r"""[a-z]""")
        base = Username(re_pattern=ure)
        for _ in range(100):
            username = gen_rand_str(keyspace='abcdefghijklmnopqrstuvwxyz')
            base(username)
            self.assertTrue(base.validate())

    def test_add_reserved_username(self):
        base = Username(reserved_usernames=[])
        randlist = RandData(str).list(200)
        base.add_reserved_username(*randlist)
        for username in randlist:
            base(username)
            self.assertFalse(base.validate())

    def test_insert(self):
        self.base('jared')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, 'jared')

    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base('jared')
        self.orm.insert(self.base)
        orm = self.orm.new().desc(self.orm.uid)
        self.assertEqual(getattr(orm.get(), self.base.field_name).value,
                         self.base.value)

    def test_array_insert(self):
        arr = ['jared', 'jaredlunde']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, arr)

    def test_array_select(self):
        arr = ['jared', 'jaredlunde']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'citext')
        self.assertEqual(self.base_array.type_name, 'citext[]')


class TestEncUsername(TestUsername, TestEncChar):

    @property
    def base(self):
        return self.orm.enc_username

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')

    def test_deepcopy(self):
        pass


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestUsername,
                        TestEncUsername,
                        failfast=True,
                        verbosity=2)
