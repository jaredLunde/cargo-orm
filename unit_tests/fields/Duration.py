#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import datetime

from unit_tests.fields.Field import *
from unit_tests import configure


class TestDuration(configure.ExtrasTestCase, TestField):

    @property
    def base(self):
        return self.orm.duration

    def test___call__(self):
        self.base(6081153379)
        self.assertIsInstance(self.base.value, datetime.timedelta)
        self.base(None)
        self.assertIs(self.base.value, None)
        self.base(datetime.timedelta(seconds=608115))
        self.assertIsInstance(self.base.value, datetime.timedelta)

    def test_format(self):
        self.base(86400)
        self.assertEqual(self.base.format(), '1 day')
        self.base(86400*2)
        self.assertEqual(self.base.format(), '2 days')
        self.base(7260)
        self.assertEqual(self.base.format(), '2 hours, 1 minute')
        self.assertEqual(self.base.format('{hours:(h)} {mins:(m)}'),
                         '2h 1m')
        self.assertEqual(self.base.format('{hours:( h)} {mins:( m)}'),
                         '2 h 1 m')
        self.assertEqual(self.base.format('{hours:(h)} {mins:(min,mins)}'),
                         '2h 1min')
        self.assertEqual(self.base.format('{hours:(h)} {mins:02d(min,mins)}'),
                         '2h 01min')
        self.base(7320)
        self.assertEqual(self.base.format('{hours:(h)} {mins:(min,mins)}'),
                         '2h 2mins')
        self.assertEqual(self.base.format('{hours:02d}:{mins:02d}:{secs:02d}'),
                         '02:02:00')

    def test_insert(self):
        self.base(608115)
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, self.base.value)
        self.base(datetime.timedelta(seconds=608115))
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, self.base.value)

    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base(608115)
        self.orm.insert(self.base)
        orm = self.orm.new().desc(self.orm.uid)
        self.assertEqual(getattr(orm.get(), self.base.field_name).value,
                         self.base.value)

    def test_array_insert(self):
        arr = [608115, 608120]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, self.base_array.value)

    def test_array_select(self):
        arr = [608115, 608120]
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'double precision')
        self.assertEqual(self.base_array.type_name, 'double precision[]')

    def test_copy(self):
        fielda = self.base
        fieldb = self.base.copy()
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            if k not in {'_context', 'validator'}:
                self.assertEqual(getattr(fielda, k), getattr(fieldb, k))
            else:
                self.assertNotEqual(getattr(fielda, k), getattr(fieldb, k))
        self.assertEqual(fielda.table, fieldb.table)

        fielda = self.base
        fieldb = copy.copy(self.base)
        self.assertIsNot(fielda, fieldb)
        for k in list(fielda.__slots__):
            if k not in {'_context', 'validator'}:
                self.assertEqual(getattr(fielda, k), getattr(fieldb, k))
            else:
                self.assertNotEqual(getattr(fielda, k), getattr(fieldb, k))
        self.assertEqual(fielda.table, fieldb.table)


class TestEncDuration(TestDuration):

    @property
    def base(self):
        return self.orm.enc_duration

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestDuration,
                        TestEncDuration,
                        failfast=True,
                        verbosity=2)
