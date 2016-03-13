#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from unit_tests.fields.Time import TestTime
from unit_tests import configure


class TestDate(TestTime):

    @property
    def base(self):
        return self.orm.date


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestDate, failfast=True, verbosity=2)
