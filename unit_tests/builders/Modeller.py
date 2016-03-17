#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.builder.Modeller`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest

from cargo import ORM
from cargo.builder import Modeller, create_models



banner = '''
#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""

  `Cargo SQL Builder`
  ``Creates models from tables and tables from models``
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   The MIT License (MIT) © 2015 Jared Lunde
   http://github.com/jaredlunde/Cargo ORM

"""'''


class TestModeller(unittest.TestCase):
    orm = ORM()

    '''def test_output_to_file(self):
        create_models(self.orm,
                      schema='xfaps',
                      output_to='/home/jared/apps/xfaps/tests/models.py',
                      write_mode='w')

    def test_output_to_string(self):
        print(create_models(self.orm,
                            schema='xfaps'))

    def test_output_to_string_tables(self):
        print(create_models(self.orm, 'users', 'test',
                            schema='xfaps'))'''


if __name__ == '__main__':
    # Unit test
    unittest.main()
