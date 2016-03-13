#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import datetime
from dateutil import tz

import arrow
from bloom import *
from bloom.fields import Time

from unit_tests.fields.Field import *
from unit_tests import configure


class TestTime(configure.DateTimeTestCase, TestField):

    @property
    def base(self):
        return self.orm.time

    def test___call__(self):
        dts = []
        ars = []
        for phrase in ['January 26 at 11:16am EST',
                       'January 27 at 11:16am EST',
                       'January 28 at 11:16am EST']:
            self.base(phrase)
            self.assertIsInstance(self.base.value, arrow.Arrow)
            self.assertEqual(self.base.day, int(phrase.split(' ')[1]))
            self.assertIsInstance(self.base.value, arrow.Arrow)
            dts.append(self.base.value)
            ars.append(self.base())

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

    def test_value(self):
        self.base('October 31, 2014 at 11:43am')
        self.assertIsInstance(self.base.value, arrow.Arrow)

    def test_insert(self):
        self.base('October 31, 2014 at 11:43am')
        val = getattr(self.orm.insert(self.base), self.base.field_name)
        self.assertEqual(val.value, self.base.value)

    def test_select(self):
        self.base('11:43am')
        self.orm.insert(self.base),
        val = getattr(self.orm.new().desc(self.orm.uid).get(),
                      self.base.field_name)
        self.assertEqual(val.value, self.base.value)


class TestTimeTZ(TestTime):

    @property
    def base(self):
        return self.orm.timetz


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestTime, TestTimeTZ, failfast=True, verbosity=2)
