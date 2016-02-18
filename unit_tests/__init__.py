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
from unit_tests.sql.exceptions.QueryError import *
from unit_tests.sql.exceptions.ValidationError import *
from unit_tests.sql.expressions.ArrayItems import *
from unit_tests.sql.expressions.BaseExpression import *
from unit_tests.sql.expressions.BaseLogic import *
from unit_tests.sql.expressions.BaseNumericLogic import *
from unit_tests.sql.expressions.Case import *
from unit_tests.sql.expressions.Clause import *
from unit_tests.sql.expressions.DateLogic import *
from unit_tests.sql.expressions.DateTimeLogic import *
from unit_tests.sql.expressions.Expression import *
from unit_tests.sql.expressions.Function import *
from unit_tests.sql.expressions.Functions import *
from unit_tests.sql.expressions.NumericLogic import *
from unit_tests.sql.expressions.StringLogic import *
from unit_tests.sql.expressions.Subquery import *
from unit_tests.sql.expressions.TimeLogic import *
from unit_tests.sql.expressions.alias import *
from unit_tests.sql.fields.Array import *
from unit_tests.sql.fields.AuthKey import *
from unit_tests.sql.fields.BigInt import *
from unit_tests.sql.fields.BigSerial import *
from unit_tests.sql.fields.Bool import *
from unit_tests.sql.fields.Char import *
from unit_tests.sql.fields.Date import *
from unit_tests.sql.fields.Decimal import *
from unit_tests.sql.fields.Email import *
from unit_tests.sql.fields.Enum import *
from unit_tests.sql.fields.Field import *
from unit_tests.sql.fields.Float import *
from unit_tests.sql.fields.IP import *
from unit_tests.sql.fields.Int import *
from unit_tests.sql.fields.JSON import *
from unit_tests.sql.fields.JSONb import *
from unit_tests.sql.fields.Numeric import *
from unit_tests.sql.fields.Password import *
from unit_tests.sql.fields.Serial import *
from unit_tests.sql.fields.Slug import *
from unit_tests.sql.fields.SmallInt import *
from unit_tests.sql.fields.StrUID import *
from unit_tests.sql.fields.Text import *
from unit_tests.sql.fields.Time import *
from unit_tests.sql.fields.Timestamp import *
from unit_tests.sql.fields.UID import *
from unit_tests.sql.fields.UUID import *
from unit_tests.sql.fields.Username import *
from unit_tests.sql.fields.Varchar import *
from unit_tests.sql.fields._EmptyField import *
from unit_tests.sql.fields.get_pwd_context import *
from unit_tests.sql.orm.Model import *
from unit_tests.sql.orm.QueryState import *
from unit_tests.sql.orm.RestModel import *
from unit_tests.sql.orm.SQL import *
from unit_tests.sql.relationships.BaseRelationship import *
from unit_tests.sql.relationships.ForeignKey import *
from unit_tests.sql.relationships.Relationship import *
from unit_tests.sql.relationships._get_obj_from_string import *
from unit_tests.sql.statements.BaseQuery import *
from unit_tests.sql.statements.DELETE import *
from unit_tests.sql.statements.EXCEPT import *
from unit_tests.sql.statements.INSERT import *
from unit_tests.sql.statements.INTERSECT import *
from unit_tests.sql.statements.Intersections import *
from unit_tests.sql.statements.Query import *
from unit_tests.sql.statements.RAW import *
from unit_tests.sql.statements.SELECT import *
from unit_tests.sql.statements.UNION import *
from unit_tests.sql.statements.UPDATE import *
from unit_tests.sql.statements.WITH import *
from unit_tests.sql.validators.Validate import *
from unit_tests.sql.validators.ValidationValue import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
