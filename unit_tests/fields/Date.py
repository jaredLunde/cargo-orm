#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from unit_tests.fields.Time import TestTime
from unit_tests import configure


class TestDate(TestTime):

    @property
    def base(self):
        return self.orm.date

    def test_array_insert(self):
        arr = ['October 13, 2014', 'October 21, 2014']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, self.base_array.value)

    def test_array_select(self):
        arr = ['October 13, 2014', 'October 21, 2014']
        self.base_array(arr)
        val = getattr(self.orm.naked().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.naked().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val, val_b)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'date')
        self.assertEqual(self.base_array.type_name, 'date[]')


class TestEncDate(TestDate):
    @property
    def base(self):
        return self.orm.enc_ts

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestDate, TestEncDate, failfast=True, verbosity=2)
