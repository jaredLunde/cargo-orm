#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest
import datetime
from dateutil import tz

import arrow
from vital.docr import Docr
from bloom import *
from bloom.fields import Timestamp

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Time import *


class TestTimestamp(TestTime):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Timestamp()
        self.base.table = 'test'
        self.base.field_name = 'time'

    def test_descriptors(self):
        self.base('October 31, 1984 at 11:17am')
        d = Docr('bloom.Timestamp')
        for attr, obj in d.data_descriptors.items():
            pass

    def test_real_value(self):
        self.assertIsInstance(self.base.real_value, datetime.datetime)


if __name__ == '__main__':
    # Unit test
    unittest.main()
