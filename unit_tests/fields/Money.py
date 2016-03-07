#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Money

from unit_tests.fields.Currency import TestCurrency
from unit_tests import configure


class TestMoney(TestCurrency):
    '''
    value: value to populate the field with
    not_null: bool() True if the field cannot be Null
    primary: bool() True if this field is the primary key in your table
    unique: bool() True if this field is a unique index in your table
    index: bool() True if this field is a plain index in your table, that is,
        not unique or primary
    default: default value to set the field to
    validation: callable() custom validation plugin, must return True if the
        field validates, and False if it does not
    digits: int() maximum digit precision
    '''
    @property
    def base(self):
        return self.orm.money


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestMoney, failfast=True, verbosity=2)
