#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
from bloom.fields import Slug, SlugFactory

from unit_tests.fields.Field import TestField
from unit_tests import configure


class TestSlug(configure.ExtrasTestCase, TestField):

    @property
    def base(self):
        return self.orm.slug

    def test___call__(self):
        subjects = [
            'Lazy Saturday',
            'The quick brown fox jumped over the lazy dog',
            '283250@$)%*2350423-jGWG)NGP#@G'
        ]
        expected = [
            'lazy-saturday',
            'the-quick-brown-fox-jumped-over-the-lazy-dog',
            '283250-2350423-jgwg-ngp-g'
        ]
        for subject, match in zip(subjects, expected):
            self.base(subject)
            self.assertEqual(self.base(), match)
        for subject, match in zip(expected, expected):
            self.base(subject)
            self.assertEqual(self.base(), match)

    def test_slug_factory(self):
        factory = SlugFactory(max_length=5, word_boundary=False)
        subjects = [
            'Lazy Saturday',
            'The quick brown fox jumped over the lazy dog',
            '283250@$)%*2350423-jGWG)NGP#@G'
        ]
        base = Slug(slug_factory=factory)
        for subject in subjects:
            base(subject)
            self.assertLessEqual(len(self.base.value), 5)
        factory = SlugFactory(separator='**')
        base = Slug(slug_factory=factory)
        for subject in subjects:
            base(subject)
            self.assertIn("**", base())

    def test_insert(self):
        self.base('Lazy Saturday')
        val = getattr(self.orm.new().insert(self.base), self.base.field_name)
        self.assertEqual(val.value, self.base.value)

    def test_select(self):
        self.assertIs(self.base.value, self.base.empty)
        self.base('Lazy Saturday')
        self.orm.insert(self.base)
        val = self.orm.new().desc(self.orm.uid).get()
        self.assertEqual(getattr(val, self.base.field_name).value,
                         self.base.value)

    def test_array_insert(self):
        arr = ['Lazy Saturday', 'The quick brown fox jumped over the lazy dog']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        self.assertListEqual(val.value, self.base_array.value)

    def test_array_select(self):
        arr = ['Lazy Saturday', 'The quick brown fox jumped over the lazy dog']
        self.base_array(arr)
        val = getattr(self.orm.new().insert(self.base_array),
                      self.base_array.field_name)
        val_b = getattr(self.orm.new().desc(self.orm.uid).get(),
                        self.base_array.field_name)
        self.assertListEqual(val.value, val_b.value)

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')


class TestEncSlug(TestSlug):

    @property
    def base(self):
        return self.orm.enc_slug

    def test_init(self):
        pass

    def test_type_name(self):
        self.assertEqual(self.base.type_name, 'text')
        self.assertEqual(self.base_array.type_name, 'text[]')

    def test_deepcopy(self):
        pass


if __name__ == '__main__':
    # Unit test
    configure.run_tests(TestSlug, TestEncSlug, failfast=True, verbosity=2)
