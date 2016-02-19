#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for bloom.clients.create_client`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import json
import unittest
from kola import config

from bloom.clients import create_client, create_kola_client, local_client,\
                              Postgres, create_db, create_kola_db


cfile = '/home/jared/apps/xfaps/vital.json'


class Testcreate_client(unittest.TestCase):

    with open(cfile, 'r') as f:
        config = json.load(f)

    def test_create_db(self):
        pass

if __name__ == '__main__':
    # Unit test
    unittest.main()
