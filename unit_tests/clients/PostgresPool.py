#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for vital.sql.clients.PostgresPool`
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
from vital.sql.clients import Postgres, PostgresPool


cfile = '/home/jared/apps/xfaps/vital.json'


class TestPostgresPool(unittest.TestCase):
    with open(cfile, 'r') as f:
        config = json.load(f)

    def test_connect(self):
        client = PostgresPool()
        self.assertFalse(client.autocommit)
        self.assertIsNone(client._pool)
        self.assertDictEqual(client._connection_options, {})
        self.assertIsNone(client._schema)
        self.assertEqual(client.encoding, None)
        self.assertEqual(client.cursor_factory, CNamedTupleCursor)

    def test_connection(self):
        cfg = self.config.get('db')
        dsn_config = Postgres.to_dsn(cfg)
        client = PostgresPool(1, 2, dsn_config)
        conn = client.get()
        self.assertFalse(conn._connection.closed)
        conn.close()
        self.assertTrue(conn._connection.closed)

        client = PostgresPool(1, 2, **cfg)
        conn = client.get()
        self.assertFalse(conn._connection.closed)

    def test_close(self):
        client = PostgresPool(1, 2, **self.config.get('db', {}))
        self.assertTrue(client.closed)
        client.connect()
        self.assertFalse(client.closed)
        client.close()
        self.assertTrue(client.closed)

    def test_context_manager(self):
        with PostgresPool(1, 2, **self.config.get('db', {})) as pool:
            self.assertFalse(pool.closed)
            with pool.get() as connection:
                with pool.get() as connection2:
                    self.assertIsNot(connection, connection2)
                    with self.assertRaises(psycopg2.pool.PoolError):
                        with pool.get() as connection3:
                            pass
                with pool.get() as connection4:
                    self.assertIsNot(connection, connection4)
                    self.assertIs(connection2.connection,
                                  connection4.connection)
                    with self.assertRaises(psycopg2.pool.PoolError):
                        with pool.get() as connection5:
                            pass
        self.assertTrue(pool.closed)

    def test_connection_obj(self):
        with PostgresPool(1, 2, **self.config.get('db', {})) as pool:
            with pool.get() as connection:
                self.assertIs(connection.autocommit, pool.autocommit)
                self.assertIs(connection._dsn, pool._dsn)
                self.assertIs(connection._schema, pool._schema)
                self.assertIs(connection.encoding, pool.encoding)
                self.assertIs(connection.minconn, pool.minconn)
                self.assertIs(connection.maxconn, pool.maxconn)
                self.assertIs(connection.cursor_factory, pool.cursor_factory)

    def test_put(self):
        with PostgresPool(1, 2, **self.config.get('db', {})) as pool:
            conn = pool.get()
            self.assertIsNotNone(conn._connection)
            conn2 = pool.get()
            self.assertIsNot(conn2, conn)
            with self.assertRaises(psycopg2.pool.PoolError):
                pool.get()
            # Put conn obj
            pool.put(conn)
            conn2 = pool.get()
            self.assertIsNotNone(conn2)
            # Put raw conn
            pool.put(conn2.connection)
            conn2 = pool.get()
            self.assertIsNotNone(conn2)
        self.assertTrue(pool.closed)
        self.assertTrue(conn.closed)
        self.assertTrue(conn2.closed)

    def test_commit(self):
        client = PostgresPool(1, 2, **self.config.get('db', {}))
        conn = client.get()
        cur = conn.cursor()
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        self.assertIsNone(conn.commit())
        cur = conn.cursor()
        with self.assertRaises(psycopg2.ProgrammingError):
            cur.execute(
                "INSERT INTO foo (uid, textfield) VALUES (1, 'bar', 4)")
            conn.commit()
        with self.assertRaises(psycopg2.InternalError):
            cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        client.put(conn)

    def test_rollback(self):
        client = PostgresPool(1, 2, **self.config.get('db', {}))
        conn = client.get()
        cur = conn.cursor()
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        self.assertIsNone(conn.commit())
        cur = conn.cursor()
        with self.assertRaises(psycopg2.ProgrammingError):
            cur.execute(
                "INSERT INTO foo (uid, textfield) VALUES (1, 'bar', 4)")
            conn.commit()
        with self.assertRaises(psycopg2.InternalError):
            cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        conn.rollback()
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        self.assertIsNone(conn.commit())
        client.put(conn)

    def test_minconn_maxconn(self):
        client = PostgresPool(10, 12, **self.config.get('db', {}))
        self.assertEqual(client.pool.minconn, 10)
        self.assertEqual(client.pool.maxconn, 12)


if __name__ == '__main__':
    # Unit test
    unittest.main()
