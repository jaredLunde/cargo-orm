#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import uuid
import unittest

from bloom.fields import UUID

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.fields.Field import *


class TestUUID(TestField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        id = uuid.uuid4()
        self.base = UUID(id)
        self.assertEqual(self.base(), id)
        self.assertEqual(self.base.value, id)


if __name__ == '__main__':
    # Unit test
    unittest.main()
