#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from unit_tests.fields.Time import TestTime
from unit_tests import configure


class TestTimestamp(TestTime):

    @property
    def base(self):
        return self.orm.ts


class TestTimestampTZ(TestTimestamp):

    @property
    def base(self):
        return self.orm.tstz


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestTimestamp,
                        TestTimestampTZ,
                        failfast=True,
                        verbosity=2)
