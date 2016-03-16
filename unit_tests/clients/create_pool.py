#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.clients.create_pool`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import json
import unittest
from multiprocessing import cpu_count

from kola import config
from cargo.clients import create_pool, create_kola_pool, local_client,\
                              PostgresPool


cfile = '/home/jared/apps/xfaps/vital.json'


class Testcreate_pool(unittest.TestCase):

    with open(cfile, 'r') as f:
        config = json.load(f)

    def test_create_with_opt(self):
        local_client.clear()
        pool = create_pool(**config)
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], pool)
        self.assertIsInstance(pool, PostgresPool)

        pool2 = create_pool(name='db2', **config)
        self.assertIn('db2', local_client)
        self.assertIs(local_client['db2'], pool2)
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], pool)

        self.assertEqual(cpu_count(), pool.minconn)
        self.assertEqual(cpu_count()*2, pool.maxconn)

    def test_vital_create(self):
        local_client.clear()
        pool = create_kola_pool()
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], pool)
        self.assertIsInstance(pool, PostgresPool)
        
        pool2 = create_kola_pool(name='db2')
        self.assertIn('db2', local_client)
        self.assertIs(local_client['db2'], pool2)

        self.assertEqual(cpu_count(), pool.minconn)
        self.assertEqual(cpu_count()*2, pool.maxconn)


if __name__ == '__main__':
    # Unit test
    unittest.main()
