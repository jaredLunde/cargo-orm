#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')

from vital import config
from unit_tests.sql.clients.PostgresClient import *
from unit_tests.sql.clients.PostgresPool import *
from unit_tests.sql.clients.create_client import *
from unit_tests.sql.clients.create_pool import *

if __name__ == '__main__':
    # Unit test
    unittest.main()
