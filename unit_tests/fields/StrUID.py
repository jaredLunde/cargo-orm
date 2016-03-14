#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom import Function
from bloom.fields import StrUID

from unit_tests.fields.UID import TestUID
from unit_tests import configure


class TestStrUID(TestUID):
    orm = configure.StrUIDModel()

    @property
    def base(self):
        return self.orm.uid

    def test_init(self):
        base = StrUID()
        self.assertEqual(base.value, base.empty)
        self.assertTrue(base.primary)
        self.assertIsNone(base.unique)
        self.assertIsNone(base.index)
        self.assertIsInstance(base.default, Function)
        self.assertIsNone(base.not_null)

    def test___call__(self):
        for check in [2223372036854775807, '2223372036854775808']:
            self.base(check)
            self.assertNotEqual(self.base(), int(check))
            self.assertEqual(self.base.value, int(check))
            self.assertNotEqual(str(self.base), int(self.base))
            self.assertEqual(str(self.base), self.base())
            self.assertEqual(int(self.base), self.base.value)
        self.assertEqual(self.base(12345678), self.base('12345678'))


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestStrUID, verbosity=2, failfast=True)
