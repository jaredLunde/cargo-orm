#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for vital.sql.builder.Modeller`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from vital import config
from vital.sql import ORM, create_vital_client
from vital.sql.builder import Modeller, create_models


cfile = '/home/jared/apps/xfaps/vital.json'
config.bind(cfile)

create_vital_client()


banner = '''
#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Vital SQL Builder`
  ``Creates models from tables and tables from models``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/VitalSQL

"""'''


class TestModeller(unittest.TestCase):
    orm = ORM()

    def test_output_to_file(self):
        create_models(self.orm,
                      schema='xfaps',
                      output_to='/home/jared/apps/xfaps/tests/models.py',
                      write_mode='w')

    def test_output_to_string(self):
        print(create_models(self.orm,
                            schema='xfaps'))

    def test_output_to_string_tables(self):
        print(create_models(self.orm, 'users', 'test',
                            schema='xfaps'))


if __name__ == '__main__':
    # Unit test
    unittest.main()
