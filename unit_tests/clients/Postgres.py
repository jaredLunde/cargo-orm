#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for vital.sql.clients.Postgres`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
try:
    import ujson as json
except:
    import json

import unittest
import psycopg2

from vital import config
from vital.sql.cursors import *
from vital.sql.clients import Postgres


cfile = '/home/jared/apps/xfaps/vital.json'


class TestPostgres(unittest.TestCase):
    with open(cfile, 'r') as f:
        config = json.load(f)

    def test_connect(self):
        client = Postgres()
        self.assertFalse(client.autocommit)
        self.assertIsNone(client._connection)
        self.assertDictEqual(client._connection_options, {})
        self.assertIsNone(client._schema)
        self.assertEqual(client.encoding, None)
        self.assertEqual(client.cursor_factory, CNamedTupleCursor)

    def test_connection(self):
        cfg = self.config.get('db')
        dsn_config = Postgres.to_dsn(cfg)
        client = Postgres(dsn_config)
        client.connection
        self.assertFalse(client._connection.closed)
        client.close()
        self.assertTrue(client._connection.closed)

        client = Postgres(**cfg)
        client.connection
        self.assertFalse(client._connection.closed)

    def test_close(self):
        client = Postgres(**self.config.get('db', {}))
        self.assertTrue(client.closed)
        client.connect()
        self.assertFalse(client.closed)
        client.close()
        self.assertTrue(client.closed)

    def test_context_manager(self):
        with Postgres(**self.config.get('db', {})) as client:
            self.assertFalse(client.closed)
        self.assertTrue(client.closed)

    def test_commit(self):
        client = Postgres(**self.config.get('db', {}))
        cur = client.cursor()
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        self.assertIsNone(client.commit())
        cur = client.cursor()
        with self.assertRaises(psycopg2.ProgrammingError):
            cur.execute(
                "INSERT INTO foo (uid, textfield) VALUES (1, 'bar', 4)")
            client.commit()
        with self.assertRaises(psycopg2.InternalError):
            cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")

    def test_rollback(self):
        client = Postgres(**self.config.get('db', {}))
        cur = client.cursor()
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        self.assertIsNone(client.commit())
        cur = client.cursor()
        with self.assertRaises(psycopg2.ProgrammingError):
            cur.execute(
                "INSERT INTO foo (uid, textfield) VALUES (1, 'bar', 4)")
            client.commit()
        with self.assertRaises(psycopg2.InternalError):
            cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        client.rollback()
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        self.assertIsNone(client.commit())


if __name__ == '__main__':
    # Unit test
    unittest.main()
