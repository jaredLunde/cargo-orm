#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.statements.Update`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import random

from cargo import *
from cargo.statements import Update
from unit_tests import configure
from unit_tests.configure import new_field, new_clause, new_expression, \
                                 new_function


randint = random.randint


class TestUpdate(configure.StatementTestCase):


    def test___init__(self):
        self.orm.fields.append(self.orm.fields[1] == 'uid')
        self.orm.set(*self.orm.fields)
        q = Update(self.orm)
        self.assertIs(q.orm, self.orm)
        self.orm.fields.pop(-1)

    def test_execute(self):
        self.orm.fields[1].value = 'bar_2'
        self.orm.where(self.orm.fields[0] == 1236).set(*[
            field == field.value for field in self.orm.fields])
        self.orm.returning(*self.orm.fields)
        q = Update(self.orm)
        print()
        result = q.execute().fetchall()
        self.assertTrue(len(result) > 0)
        self.orm.reset()

        self.orm.fields[1].value = 'bar'
        self.orm.where(self.orm.fields[0] == 1236).set(*[
            field == field.value for field in self.orm.fields])
        self.orm.returning(*self.orm.fields[:1])
        q = Update(self.orm)
        result = q.execute().fetchall()
        self.assertTrue(len(result) > 0)

    def test_evaluate_state(self):
        '''
        if state.clause == "SET":
            query_clauses[0] = state.string
        elif state.clause == "FROM":
            query_clauses[1] = state.string
        elif state.clause == "WHERE":
            query_clauses[2] = state.string
        elif state.clause == "RETURNING":
            query_clauses[3] = state.string
        '''
        clauses = [
            new_clause('SET', safe("textfield = b.textfield")),
            new_clause('FROM', safe('foo_b b')),
            new_clause('WHERE', safe('foo.uid = b.uid')),
            new_clause('RETURNING', safe('*'))
        ]
        shuffled_clauses = clauses.copy()
        random.shuffle(shuffled_clauses)
        self.orm.state.add(*shuffled_clauses)
        q = Update(self.orm)
        self.assertTrue(len(q.execute().fetchall()) > 1)

        clause_str = " ".join(q.evaluate_state())
        for clause in clauses:
            self.assertIn(clause.clause, clause_str)
        self.orm.reset()

        field_b = new_field('int', 1234, name='uid', table='foo_b')
        field_b.set_alias(table='b')
        self.orm.where(self.orm.fields[0] == aliased(field_b))
        self.orm.set(
            self.orm.fields[0] == aliased(field_b),
            self.orm.fields[1] == self.orm.fields[1].value)
        self.orm.use(field_b.table, alias='b')
        self.orm.returning(aliased(field_b))
        q = Update(self.orm)
        result = q.execute().fetchall()
        self.assertTrue(len(result) > 1)
        self.assertIn('uid', result[0]._fields)

    '''def test_pickle(self):
        field_b = new_field('int', value=1234, name='uid', table='foo_b')
        field_b.set_alias(table='b')
        self.orm.where(self.orm.fields[0] == aliased(field_b))
        self.orm.set(
            self.orm.fields[0] == aliased(field_b),
            self.orm.fields[1] == self.orm.fields[1].value)
        self.orm.use(field_b.table, alias='b')
        self.orm.returning(aliased(field_b))
        q = Update(self.orm)
        b = pickle.loads(pickle.dumps(q))
        for k in dir(q):
            if k == '_client':
                continue
            if isinstance(
               getattr(q, k), (str, list, tuple, dict, int, float)):
                self.assertEqual(getattr(q, k), getattr(b, k))
            else:
                self.assertTrue(
                    getattr(q, k).__class__ == getattr(b, k).__class__)'''


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestUpdate)
