# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from caches.compat import *
import caches
from caches.core import Cache

from . import TestCase


class CachesTest(TestCase):
    def test_clear_unsafe(self):
        Cache("foo", 1, prefix="foo")
        Cache("bar", 1, prefix="che")
        Cache("bar", 1, prefix="foo")

        c = Cache("foo", prefix="foo")
        self.assertTrue(c.exists())

        count = caches.clear_unsafe("foo*")
        self.assertEqual(2, count)

        c = Cache("foo", prefix="foo")
        self.assertFalse(c.exists())

