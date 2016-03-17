#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.clients.Postgres`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import unittest
import psycopg2

from cargo.cursors import *
from cargo.clients import Postgres, local_client

from unit_tests import configure


class TestPostgres(unittest.TestCase):
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

    def test_connect(self):
        client = Postgres()
        self.assertFalse(client.autocommit)
        self.assertIsNone(client._connection)
        self.assertDictEqual(client._connection_options, {})
        self.assertIsNone(client._schema)
        self.assertEqual(client.encoding, None)
        self.assertEqual(client.cursor_factory, CNamedTupleCursor)

    def test_connection(self):
        client = Postgres()
        client.connection
        self.assertFalse(client._connection.closed)
        client.close()
        self.assertTrue(client._connection.closed)

        client = Postgres()
        client.connection
        self.assertFalse(client._connection.closed)

    def test_close(self):
        client = Postgres()
        self.assertTrue(client.closed)
        client.connect()
        self.assertFalse(client.closed)
        client.close()
        self.assertTrue(client.closed)

    def test_context_manager(self):
        with Postgres() as client:
            self.assertFalse(client.closed)
        self.assertTrue(client.closed)

    def test_commit(self):
        client = Postgres()
        cur = client.cursor()
        client.apply_schema(cur, 'cargo_tests')
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
        client = Postgres()
        cur = client.cursor()
        client.apply_schema(cur, 'cargo_tests')
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (2, 'bar')")
        self.assertIsNone(client.commit())
        cur = client.cursor()
        with self.assertRaises(psycopg2.ProgrammingError):
            cur.execute(
                "INSERT INTO foo (uid, textfield) VALUES (1, 'bar', 4)")
            client.commit()
        with self.assertRaises(psycopg2.InternalError):
            cur.execute("INSERT INTO foo (uid, textfield) VALUES (1, 'bar')")
        client.rollback()
        cur.execute("INSERT INTO foo (uid, textfield) VALUES (3, 'bar')")
        self.assertIsNone(client.commit())

    def test_get_oid(self):
        client = Postgres()
        OIDs = client.get_type_OID('text')
        self.assertSequenceEqual(OIDs, (25, 1009))

    def _is_client(self):
        return isinstance(x, client)

    def test_before_event(self):
        client = Postgres()
        for event in client.EVENTS:
            client.before(event, self._is_client)
        for event in client.EVENTS:
            cb = client._events['BEFORE'][event]
            self.assertIsInstance(cb, list)
            self.assertEqual(cb[0], self._is_client)
        client.before('COMMIT', self._is_client)
        self.assertEqual(len(client._events['BEFORE']['COMMIT']), 1)
        client.before('COMMIT', self.test_get_oid)
        self.assertEqual(len(client._events['BEFORE']['COMMIT']), 2)
        self.assertEqual(client._events['BEFORE']['COMMIT'].pop(),
                         self.test_get_oid)

    def test_after_event(self):
        client = Postgres()
        for event in client.EVENTS:
            client.after(event, self._is_client)
        for event in client.EVENTS:
            cb = client._events['AFTER'][event]
            self.assertIsInstance(cb, list)
            self.assertEqual(cb[0], self._is_client)
        client.after('COMMIT', self._is_client)
        self.assertEqual(len(client._events['AFTER']['COMMIT']), 1)
        client.after('COMMIT', self.test_get_oid)
        self.assertEqual(len(client._events['AFTER']['COMMIT']), 2)
        self.assertEqual(client._events['AFTER']['COMMIT'].pop(),
                         self.test_get_oid)

if __name__ == '__main__':
    # Unit test
    unittest.main()
