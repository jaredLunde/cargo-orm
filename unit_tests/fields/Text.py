#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Text

from unit_tests.fields.Varchar import TestVarchar
from unit_tests import configure


class TestText(TestVarchar):

    @property
    def base(self):
        return self.orm.text

    def test_select(self):
        self.base('foo')
        self.orm.insert(self.base)
        r = getattr(self.orm.new().desc(self.orm.uid).get(self.base),
                    self.base.field_name)
        self.assertEqual(r.value, 'foo')


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestText, failfast=True, verbosity=2)
