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
                              Postgres


cfile = '/home/jared/apps/xfaps/vital.json'


class Testcreate_client(unittest.TestCase):

    with open(cfile, 'r') as f:
        config = json.load(f)

    def test_create_with_opt(self):
        local_client.clear()
        client = create_client(**config)
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], client)
        self.assertIsInstance(client, Postgres)

        client2 = create_client(name='db2', **config)
        self.assertIn('db2', local_client)
        self.assertIs(local_client['db2'], client2)
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], client)

    def test_vital_create(self):
        local_client.clear()
        client = create_kola_client()
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], client)
        self.assertIsInstance(client, Postgres)

        client2 = create_kola_client(name='db2')
        self.assertIn('db2', local_client)
        self.assertIs(local_client['db2'], client2)

if __name__ == '__main__':
    # Unit test
    unittest.main()
