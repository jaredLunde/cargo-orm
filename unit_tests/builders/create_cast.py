#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.builder.create_user`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
from cargo.builder import create_cast
from cargo.builder.casts import Cast

from unit_tests import configure
from unit_tests.configure import new_field


class TestCreateCast(configure.BuilderTestCase):

    def test_create(self):
        cast = create_cast(self.orm, 'bigint', 'int8', dry=True)
        print(cast.query)
        cast = Cast(self.orm, 'citext', 'text')
        cast.as_assignment()
        cast.as_implicit()
        cast.inout()
        print(cast.query)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestCreateCast, verbosity=2, failfast=True)
