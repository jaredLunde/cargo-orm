#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from vital import config

from vital.docr import Docr
from vital.sql.fields import Bool
from vital.sql import create_pool

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Field import TestField


class TestBool(TestField):

    def test_validate(self):
        a = Bool()
        self.assertTrue(a.validate())
        a = Bool(not_null=True)
        self.assertFalse(a.validate())
        a(True)
        self.assertTrue(a.validate())
        a(False)
        self.assertTrue(a.validate())
        a(None)
        self.assertFalse(a.validate())

    def test___call__(self):
        a = Bool()
        self.assertIsNone(a())
        a(True)
        self.assertTrue(a())
        a(False)
        self.assertFalse(a())
        a(1)
        self.assertTrue(a())
        a(0)
        self.assertFalse(a())
        a('0')
        self.assertTrue(a())



if __name__ == '__main__':
    # Unit test
    unittest.main()
