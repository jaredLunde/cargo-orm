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
from unit_tests.expressions.ArrayItems import *
from unit_tests.expressions.BaseLogic import *
from unit_tests.expressions.Case import *
from unit_tests.expressions.Clause import *
from unit_tests.expressions.DateTimeLogic import *
from unit_tests.expressions.Expression import *
from unit_tests.expressions.Function import *
from unit_tests.expressions.NumericLogic import *
from unit_tests.expressions.StringLogic import *
from unit_tests.expressions.Subquery import *
from unit_tests.expressions.alias import *
from unit_tests.fields.Array import *
from unit_tests.fields.AuthKey import *
from unit_tests.fields.BigInt import *
from unit_tests.fields.BigSerial import *
from unit_tests.fields.Bool import *
from unit_tests.fields.Char import *
from unit_tests.fields.Date import *
from unit_tests.fields.Decimal import *
from unit_tests.fields.Email import *
from unit_tests.fields.Enum import *
from unit_tests.fields.Field import *
from unit_tests.fields.Float import *
from unit_tests.fields.IP import *
from unit_tests.fields.Int import *
from unit_tests.fields.JSON import *
from unit_tests.fields.JSONb import *
from unit_tests.fields.Numeric import *
from unit_tests.fields.Password import *
from unit_tests.fields.Serial import *
from unit_tests.fields.Slug import *
from unit_tests.fields.SmallInt import *
from unit_tests.fields.StrUID import *
from unit_tests.fields.Text import *
from unit_tests.fields.Time import *
from unit_tests.fields.Timestamp import *
from unit_tests.fields.UID import *
from unit_tests.fields.UUID import *
from unit_tests.fields.Username import *
from unit_tests.fields.Varchar import *
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
