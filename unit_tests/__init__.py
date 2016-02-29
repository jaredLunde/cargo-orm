#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
print(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from kola import config
from unit_tests.clients.Postgres import *
from unit_tests.clients.PostgresPool import *
from unit_tests.clients.create_client import *
from unit_tests.clients.create_pool import *
from unit_tests.exceptions.QueryError import *
from unit_tests.exceptions.ValidationError import *
from unit_tests.expressions import *
from unit_tests.fields import *
from unit_tests.orm.Model import *
from unit_tests.orm.QueryState import *
from unit_tests.orm.RestModel import *
from unit_tests.orm.SQL import *
from unit_tests.relationships.BaseRelationship import *
from unit_tests.relationships.ForeignKey import *
from unit_tests.relationships.Relationship import *
from unit_tests.statements.DELETE import *
from unit_tests.statements.INSERT import *
from unit_tests.statements.Intersections import *
from unit_tests.statements.RAW import *
from unit_tests.statements.SELECT import *
from unit_tests.statements.UPDATE import *
from unit_tests.statements.WITH import *
from unit_tests.validators.Validate import *
from unit_tests.validators.ValidationValue import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
