#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

import psycopg2.extras
from kola import request
from bloom.fields import IP

from unit_tests.fields.Field import *


request.bind()
request.environ['REMOTE_ADDR'] = '68.161.102.23'


class TestIP(TestField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = IP(request=request)
        request.bind()
        request.environ['REMOTE_ADDR'] = '68.161.102.23'

    def test___call__(self):
        self.base = IP(request=request)
        self.assertEqual(self.base.value, self.base.empty)
        self.base(request.remote_addr)
        self.assertIsInstance(self.base.value, psycopg2.extras.Inet)
        self.base(IP.current)
        self.assertEqual(str(self.base.value), request.remote_addr)

    def test_default(self):
        self.base = IP(request=request, default=IP.current)
        self.assertEqual(str(self.base.default), request.remote_addr)


if __name__ == '__main__':
    # Unit test
    unittest.main()
