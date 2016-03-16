#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
path = os.path.dirname(os.path.abspath(__file__)).split('cargo-orm')[0] + \
    'cargo-orm'
sys.path.insert(0, path)


from kola import config
from unit_tests.clients.Postgres import *
from unit_tests.clients.PostgresPool import *
from unit_tests.clients.create_client import *
from unit_tests.clients.create_pool import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
