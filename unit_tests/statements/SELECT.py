#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
"""
    `Unit tests for cargo.statements.Select`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde
"""
import pickle
import random

from cargo import *
from cargo.statements import Select

from unit_tests import configure
from unit_tests.configure import new_clause, new_field, new_expression


class TestSelect(configure.StatementTestCase):

    def test___init__(self):
        func = F.new('cargo_uid', alias="id")
        self.orm.state.fields = [func]
        q = Select(self.orm)
        self.assertIs(q.orm, self.orm)
        self.assertListEqual(list(q.fields), [func])

    def test__set_fields(self):
        func = F.new('cargo_uid', alias="id")
        fields = [func, 'fish', 1234]
        fields.extend(self.orm.fields)
        self.orm.state.fields = fields
        q = Select(self.orm)
        q.compile()
        compiled = q.query % q.params
        make_str = lambda x: x.name if hasattr(x, 'name') else str(x)
        self.assertIn(", ".join(map(make_str, fields)), compiled)
        for k, v in q.params.items():
            self.assertIn(k, q.query)
        self.orm.reset()

    def test_execute(self):
        func = F.new('cargo_uid', alias="id")
        fields = [
            func,
            parameterize('fish', alias='fish'),
            parameterize(1234, alias='fosh')]
        fields.extend(self.orm.fields)
        self.orm.from_('foo')
        self.orm.where(fields[-2] <= fields[-2]())
        self.orm.limit(1, 2)
        self.orm.state.fields = fields
        q = Select(self.orm)
        q.compile()
        compiled = q.query % q.params
        for v in fields:
            if hasattr(v, 'name'):
                v = v.name
            elif hasattr(v, 'params'):
                v = v.string % v.params
            else:
                v = str(v)
            self.assertIn(v, compiled)
        for k, v in q.params.items():
            self.assertIn(k, q.query)
        result = q.execute().fetchall()
        self.assertTrue(len(result), 2)
        self.orm.reset()

    def test__evaluate_state(self):
        clauses = [
            new_clause('FROM', safe('foo foo')),
            new_clause('WINDOW', safe('w AS (ORDER BY foo.uid DESC)')),
            new_clause('INNER JOIN', safe('foo foo2 ON foo.uid = foo2.uid')),
            new_clause('WHERE', self.orm.fields[0] > 0),
            new_clause('GROUP BY', self.orm.fields[0]),
            new_clause('HAVING', safe('max(foo.uid) < 100000000000000')),
            new_clause('ORDER BY', safe('foo.uid DESC')),
            new_clause('LIMIT', 1),
            new_clause('OFFSET', 0),
            # new_clause('FETCH', safe('NEXT 1 row ONLY')),
            # new_clause('FOR', safe('UPDATE OF foo'))
        ]
        shuffled_clauses = clauses.copy()
        random.shuffle(shuffled_clauses)

        self.orm.state.add(*shuffled_clauses)
        self.orm.state.fields = [self.orm.fields[0]]
        q = Select(self.orm)
        result = q.execute().fetchall()[0]
        self.assertDictEqual(result._asdict(), {'uid': self.orm.fields[0]()})
        self.orm.reset()

        clauses = [
            new_clause('FROM', safe('foo foo')),
            new_clause('INNER JOIN', safe('foo foo2 ON foo.uid = foo2.uid')),
            new_clause('WHERE', self.orm.fields[0] > 2),
            # new_clause('HAVING', safe('max(foo.uid) < 1235')),
            new_clause('ORDER BY', safe('foo.uid DESC')),
            # new_clause('LIMIT', 1),
            # new_clause('OFFSET', 0),
            new_clause('FETCH', safe('NEXT 1 row ONLY')),
            new_clause('FOR', safe('UPDATE OF foo'))
        ]
        shuffled_clauses = clauses.copy()
        random.shuffle(shuffled_clauses)

        self.orm.state.add(*shuffled_clauses)
        self.orm.state.fields = [self.orm.fields[0]]
        q = Select(self.orm)
        result = q.execute().fetchall()[0]
        self.assertDictEqual(result._asdict(), {'uid': 1236})
        self.orm.reset()

    '''def test_pickle(self):
        self.orm.where(safe('true') & True)
        self.orm.state.fields = [self.orm.fields[0]]
        q = Select(self.orm)
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
    configure.run_tests(TestSelect, failfast=True, verbosity=2)
