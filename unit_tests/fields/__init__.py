#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import unittest
path = os.path.dirname(os.path.abspath(__file__)).split('bloom-orm')[0] + \
    'bloom-orm'
sys.path.insert(0, path)


from kola import config
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


if __name__ == '__main__':
    # Unit test
    unittest.main()
