#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')


from kola import config
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
from unit_tests.sql.fields.get_pwd_context import *


if __name__ == '__main__':
    # Unit test
    unittest.main()
