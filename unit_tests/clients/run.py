#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')

from kola import config
from unit_tests.clients.PostgresClient import *
from unit_tests.clients.PostgresPool import *
from unit_tests.clients.create_client import *
from unit_tests.clients.create_pool import *

if __name__ == '__main__':
    # Unit test
    unittest.main()
