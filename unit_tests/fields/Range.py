#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import arrow
import datetime
import decimal
from cargo.fields.ranges import *

from unit_tests.fields.Field import TestField
from unit_tests import configure

from psycopg2.extras import Range, DateRange, DateTimeRange, DateTimeTZRange,\
                            NumericRange, Range


class TestIntRange(configure.RangeTestCase, TestField):

    @property
    def base(self):
        return self.orm.integer

    def test_validate(self):
        base = self.base.__class__(not_null=True)
        self.assertFalse(base.validate())
        base(None)
        self.assertFalse(base.validate())
        base((10, 20))
        self.assertTrue(base.validate())

        base = self.base.__class__()
        self.assertTrue(base.validate())
        base(None)
        self.assertTrue(base.validate())
        base((10, 20))
        self.assertTrue(base.validate())

    def test___call__(self):
        self.assertIs(self.base.value, self.base.empty)
        self.assertIsNone(self.base.upper)
        self.assertIsNone(self.base.lower)
        self.base((1, 3))
        self.assertIsInstance(self.base.value, NumericRange)
        self.assertEqual(self.base.upper, 3)
        self.assertEqual(self.base.lower, 1)
        self.base(None)
        self.assertIsNone(self.base.value)

    def test_insert(self):
        self.base((1, 3))
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertIsInstance(val, NumericRange)
        self.assertEqual(val.upper, 3)
        self.assertEqual(val.lower, 1)

        self.base((None, 3))
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertIsInstance(val, NumericRange)
        self.assertEqual(val.upper, 3)
        self.assertEqual(val.lower, None)

    def test_select(self):
        self.base((1, 3))
        self.orm.insert(self.base)
        val = getattr(self.orm.new().desc(self.orm.uid).get(),
                      self.base.field_name)
        self.assertIsInstance(val, self.base.__class__)
        self.assertEqual(val.upper, 3)
        self.assertEqual(val.lower, 1)

        self.base((None, 4))
        self.orm.insert(self.base)
        val = getattr(self.orm.new().desc(self.orm.uid).get(),
                      self.base.field_name)
        self.assertIsInstance(val, self.base.__class__)
        self.assertEqual(val.upper, 4)
        self.assertIsNone(val.lower)

    def test_array_insert(self):
        arr = [(1, 3), (2, 4)]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, self.base_array.value)

    def test_array_select(self):
        arr = [(1, 3), (2, 4)]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'int4range')
        self.assertEqual(self.base_array.type_name, 'int4range[]')


class TestBigIntRange(TestIntRange):

    @property
    def base(self):
        return self.orm.bigint

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'int8range')
        self.assertEqual(self.base_array.type_name, 'int8range[]')


class TestNumericRange(TestIntRange):

    @property
    def base(self):
        return self.orm.numeric

    def test___call__(self):
        self.assertIs(self.base.value, self.base.empty)
        self.assertIsNone(self.base.upper)
        self.assertIsNone(self.base.lower)
        self.base((1.0, 3.0))
        self.assertIsInstance(self.base.value, NumericRange)
        self.assertEqual(self.base.upper, 3.0)
        self.assertEqual(self.base.lower, 1.0)
        self.assertIsInstance(self.base.lower, decimal.Decimal)
        self.assertIsInstance(self.base.upper, decimal.Decimal)
        self.base(None)
        self.assertIsNone(self.base.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'numrange')
        self.assertEqual(self.base_array.type_name, 'numrange[]')


class TestTimestampRange(TestIntRange):
    rtype = DateTimeRange

    @property
    def base(self):
        return self.orm.timestamp

    def test_validate(self):
        base = self.base.__class__(not_null=True)
        self.assertFalse(base.validate())
        base(None)
        self.assertFalse(base.validate())
        base(('October 10th at 11:39a', datetime.datetime.max))
        self.assertTrue(base.validate())

        base = self.base.__class__()
        self.assertTrue(base.validate())
        base(None)
        self.assertTrue(base.validate())
        base(('October 10th at 11:39a', datetime.datetime.max))
        self.assertTrue(base.validate())

    def test___call__(self):
        self.assertIs(self.base.value, self.base.empty)
        self.assertIsNone(self.base.upper)
        self.assertIsNone(self.base.lower)
        self.base(('October 14, 2014', 'October 31, 2014'))
        self.assertIsInstance(self.base.value, self.rtype)
        self.assertIsInstance(self.base.upper, arrow.Arrow)
        self.assertIsInstance(self.base.lower, arrow.Arrow)
        self.base(None)
        self.assertIsNone(self.base.value)

    def test_insert(self):
        self.base(('October 14, 2014 at 11:14p', 'October 31, 2014 at 11:14p'))
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertIsInstance(val, self.rtype)
        self.assertIsInstance(self.base.upper, arrow.Arrow)
        self.assertIsInstance(self.base.lower, arrow.Arrow)

        self.base((None, 'October 31, 2014'))
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertIsInstance(val, self.rtype)
        self.assertIsInstance(self.base.upper, arrow.Arrow)
        self.assertIsNone(val.lower)

        self.base((datetime.datetime.min, datetime.datetime.max))
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertIsInstance(val, self.rtype)
        self.assertIsInstance(self.base.upper, arrow.Arrow)
        self.assertIsInstance(self.base.lower, arrow.Arrow)

    def test_select(self):
        self.base(('October 14, 2014 at 11:14p', 'October 31, 2014 at 11:14p'))
        self.orm.insert(self.base)
        val = getattr(self.orm.new().desc(self.orm.uid).get(),
                      self.base.field_name)
        self.assertIsInstance(val, self.base.__class__)
        self.assertIsInstance(self.base.upper, arrow.Arrow)
        self.assertIsInstance(self.base.lower, arrow.Arrow)

        self.base((None, 'October 31, 2014'))
        self.orm.insert(self.base)
        val = getattr(self.orm.new().desc(self.orm.uid).get(),
                      self.base.field_name)
        self.assertIsInstance(val, self.base.__class__)
        self.assertIsInstance(self.base.upper, arrow.Arrow)
        self.assertIsNone(val.lower)

    def test_array_insert(self):
        arr = [('October 14, 2014 at 11:14p', 'October 31, 2014 at 11:14p')]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, self.base_array.value)

    def test_array_select(self):
        arr = [('October 14, 2014 at 11:14p', 'October 31, 2014 at 11:14p')]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'tsrange')
        self.assertEqual(self.base_array.type_name, 'tsrange[]')


class TestTimestampTZRange(TestTimestampRange):
    rtype = DateTimeTZRange

    @property
    def base(self):
        return self.orm.timestamptz

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'tstzrange')
        self.assertEqual(self.base_array.type_name, 'tstzrange[]')


class TestDateRange(TestTimestampRange):
    rtype = DateRange

    @property
    def base(self):
        return self.orm.date

    def test_array_insert(self):
        arr = [('October 14, 2014', 'October 31, 2014')]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, self.base_array.value)

    def test_array_select(self):
        arr = [('October 14, 2014', 'October 31, 2014')]
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'daterange')
        self.assertEqual(self.base_array.type_name, 'daterange[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestIntRange,
                        TestBigIntRange,
                        TestNumericRange,
                        TestTimestampRange,
                        TestTimestampTZRange,
                        TestDateRange,
                        failfast=True,
                        verbosity=2)
