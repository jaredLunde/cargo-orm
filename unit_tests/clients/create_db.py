#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.clients.create_client`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest

from cargo import ORM
from cargo.clients import db, Postgres, PostgresPool, local_client


class TestDb(unittest.TestCase):
    @staticmethod
    def tearDownClass():
        local_client.clear()
        db.open()

    def test_create_db(self):
        db.open()
        self.assertIsInstance(db.engine, ORM)
        #self.assertIsInstance(db.engine.client, PostgresPool)
        db.open(client=Postgres())
        self.assertIsInstance(db.engine.client, Postgres)
        db.open(client=PostgresPool())
        self.assertIsInstance(db.engine.client, PostgresPool)
        db.open()

if __name__ == '__main__':
    # Unit test
    unittest.main()
