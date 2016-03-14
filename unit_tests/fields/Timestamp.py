#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from unit_tests.fields.Time import TestTime
from unit_tests import configure


class TestTimestamp(TestTime):

    @property
    def base(self):
        return self.orm.ts

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'timestamp')
        self.assertEqual(self.base_array.type_name, 'timestamp[]')


class TestEncTimestamp(TestTimestamp):
    @property
    def base(self):
        return self.orm.enc_ts

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


class TestTimestampTZ(TestTimestamp):

    @property
    def base(self):
        return self.orm.tstz

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'timestamptz')
        self.assertEqual(self.base_array.type_name, 'timestamptz[]')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestTimestamp,
                        TestEncTimestamp,
                        TestTimestampTZ,
                        failfast=True,
                        verbosity=2)
