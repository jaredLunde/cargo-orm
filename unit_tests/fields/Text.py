#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Text

from unit_tests.fields.Varchar import TestVarchar
from unit_tests import configure


class TestText(TestVarchar):

    @property
    def base(self):
        return self.orm.text


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestText, failfast=True, verbosity=2)
