#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import sys
import unittest

from bloom.fields import Slug, SlugFactory

sys.path.insert(0, '/home/jared/apps/xfaps/tests/vital')
from unit_tests.sql.fields.Field import TestField


class TestSlug(TestField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base = Slug()
        self.base.table = 'test'
        self.base.field_name = 'slug'

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
        self.base = Slug(slug_factory=factory)
        for subject in subjects:
            self.base(subject)
            self.assertLessEqual(len(self.base.value), 5)
        factory = SlugFactory(separator='**')
        self.base = Slug(slug_factory=factory)
        for subject in subjects:
            self.base(subject)
            self.assertIn("**", self.base())


if __name__ == '__main__':
    # Unit test
    unittest.main()
