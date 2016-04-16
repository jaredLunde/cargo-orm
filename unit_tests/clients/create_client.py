#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.clients.create_client`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest

from cargo.clients import db, create_client, local_client, Postgres


class Testcreate_client(unittest.TestCase):
    @staticmethod
    def tearDownClass():
        local_client.clear()
        db.open()

    def test_create_with_opt(self):
        local_client.clear()
        client = create_client(host='localhost', password='')
        self.assertIn('db', local_client)
        self.assertIs(local_client['db'], client)
        self.assertIsInstance(client, Postgres)


if __name__ == '__main__':
    # Unit test
    unittest.main()
