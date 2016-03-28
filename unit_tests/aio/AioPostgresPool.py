#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.clients.AioPostgresPool`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from cargo.cursors import *
from cargo.clients import AioPostgresPool, local_client

from unit_tests.aio import configure


class TestAioPostgresPool(unittest.TestCase):
    @staticmethod
    def setUpClass():
        db = configure.db
        configure.drop_schema(db, 'cargo_tests', cascade=True, if_exists=True)
        configure.create_schema(db, 'cargo_tests')
        configure.Plan(configure.Foo()).execute()

    @staticmethod
    def tearDownClass():
        db = configure.db
        configure.drop_schema(db, 'cargo_tests', cascade=True, if_exists=True)
        local_client.clear()

    '''def test_connect(self):
        client = AioPostgresPool()
        self.assertFalse(client.autocommit)
        self.assertIsNone(client._pool)
        self.assertDictEqual(client._connection_options, {})
        self.assertIsNone(client._schema)
        self.assertEqual(client.encoding, None)
        self.assertEqual(client.cursor_factory, CNamedTupleCursor)

    def test_connection(self):
        client = AioPostgresPool(1, 2)
        conn = client.get()
        self.assertFalse(conn._connection.closed)
        conn.close()
        self.assertTrue(conn._connection.closed)

        client = AioPostgresPool(1, 2)
        conn = client.get()
        self.assertFalse(conn._connection.closed)

    def test_close(self):
        client = AioPostgresPool(1, 2)
        self.assertTrue(client.closed)
        client.connect()
        self.assertFalse(client.closed)
        client.close()
        self.assertTrue(client.closed)

    def test_context_manager(self):
        with AioPostgresPool(1, 2) as pool:
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
        with AioPostgresPool(1, 2) as pool:
            with pool.get() as connection:
                self.assertIs(connection.autocommit, pool.autocommit)
                self.assertIs(connection._dsn, pool._dsn)
                self.assertIs(connection._schema, pool._schema)
                self.assertIs(connection.encoding, pool.encoding)
                self.assertIs(connection.minconn, pool.minconn)
                self.assertIs(connection.maxconn, pool.maxconn)
                self.assertIs(connection.cursor_factory, pool.cursor_factory)

    def test_put(self):
        with AioPostgresPool(1, 2) as pool:
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
        client = AioPostgresPool(1, 2)
        conn = client.get()
        cur = conn.cursor()
        client.apply_schema(cur, 'cargo_tests')
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
        client = AioPostgresPool(1, 2)
        conn = client.get()
        cur = conn.cursor()
        client.apply_schema(cur, 'cargo_tests')
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (2, 'bar')")
        self.assertIsNone(conn.commit())
        cur = conn.cursor()
        with self.assertRaises(psycopg2.ProgrammingError):
            cur.execute(
                "INSERT INTO foo (uid, textfield) VALUES (1, 'bar', 4)")
            conn.commit()
        with self.assertRaises(psycopg2.InternalError):
            cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        conn.rollback()
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (3, 'bar')")
        self.assertIsNone(conn.commit())
        client.put(conn)

    def test_minconn_maxconn(self):
        client = AioPostgresPool(10, 12)
        self.assertEqual(client.pool.minconn, 10)
        self.assertEqual(client.pool.maxconn, 12)'''


if __name__ == '__main__':
    # Unit test
    unittest.main()
