#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest
import datetime
from dateutil import tz

import arrow
from docr import Docr
from bloom import *
from bloom.fields import Date

from unit_tests.fields.Field import *


class TestDate(TestField):
    base = Date()

    def test___call__(self):
        dts = []
        ars = []
        for phrase in ['January 26', 'January 27', 'January 28']:
            self.base(phrase)
            self.assertIsInstance(self.base.value, arrow.Arrow)
            self.assertEqual(self.base.day, int(phrase.split(' ')[1]))
            self.assertIsInstance(self.base.value, arrow.Arrow)
            dts.append(self.base.value)
            ars.append(self.base())

        for phrase in [Function('now'), Clause('now')]:
            self.base(phrase)
            self.assertIsInstance(self.base.value, phrase.__class__)
            with self.assertRaises(AttributeError):
                self.assertEqual(self.base.day, phrase)
            self.assertIsInstance(self.base.value, phrase.__class__)

        for dt in (dts + ars):
            self.base(dt)
            self.assertIsInstance(self.base(), arrow.Arrow)
            self.assertIsInstance(self.base.value, arrow.Arrow)

    def test_replace(self):
        self.base('October 31, 1984 at 11:17am')
        self.assertEqual(self.base.minute, 17)
        self.assertEqual(self.base.hour, 11)
        self.assertEqual(self.base.year, 1984)
        self.assertEqual(self.base.day, 31)
        self.base.replace(year=1987, day=30)
        self.assertEqual(self.base.year, 1987)
        self.assertEqual(self.base.day, 30)
        self.assertEqual(self.base.hour, 11)

    def test_descriptors(self):
        self.base('October 31, 1984 at 11:17am')
        d = Docr('bloom.Date')
        for attr, obj in d.data_descriptors.items():
            pass

    def test_value(self):
        self.base('October 31')
        self.assertIsInstance(self.base.value, arrow.Arrow)


if __name__ == '__main__':
    # Unit test
    unittest.main()
