#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.clients.create_pool`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
from multiprocessing import cpu_count

from cargo.clients import db, create_pool, local_client, PostgresPool


class Testcreate_pool(unittest.TestCase):

    def setUp(self):
        local_client.clear()

    @staticmethod
    def tearDownClass():
        local_client.clear()
        db.open()

    def test_create_with_opt(self):
        pool = create_pool(host='localhost', password='')
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], pool)
        self.assertIsInstance(pool, PostgresPool)

        pool2 = create_pool(name='db2')
        self.assertIn('db2', local_client)
        self.assertIs(local_client['db2'], pool2)
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], pool)

        self.assertEqual(cpu_count(), pool.minconn)
        self.assertEqual(cpu_count()*2, pool.maxconn)


if __name__ == '__main__':
    # Unit test
    unittest.main()
