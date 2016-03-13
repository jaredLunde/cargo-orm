#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import uuid

from bloom import safe
from bloom.fields import UUID

from unit_tests.fields.Field import *
from unit_tests import configure


class TestUUID(configure.IdentifierTestCase, TestField):
    orm = configure.UUIDModel()

    @property
    def base(self):
        return self.orm.uuid

    def test_init_(self):
        id = uuid.uuid4()
        base = UUID(id)
        self.assertEqual(base(), id)
        self.assertEqual(base.value, id)
        self.assertTrue(base.primary, True)

    def test_real_value(self):
        id = uuid.uuid4()
        self.base(id)
        self.assertIs(self.base.value, self.base.value)
        self.base.clear()
        self.assertIs(self.base.value, self.base.empty)
        self.base(None)
        self.assertIsNone(self.base.value)

    def test_insert(self):
        id = uuid.uuid4()
        self.base(id)
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertEqual(val, id)
        self.base.clear()
        val = getattr(self.orm.naked().insert(self.base), self.base.field_name)
        self.assertIsNotNone(val)

    def test_select(self):
        id = uuid.uuid4()
        self.base(id)
        self.orm.insert(self.base)
        self.assertEqual(
            getattr(self.orm.new().desc(self.orm.uid).get(),
                    self.base.field_name).value,
            self.base.value)

    def test_new(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base.new()
        self.assertIsNotNone(self.base.value)


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestUUID, verbosity=2, failfast=True)
