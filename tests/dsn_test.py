# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import sys
import time
import random

from . import TestCase

import caches
from caches.dsn import DSN


class DSNTest(TestCase):
    def test_scheme(self):
        dsn = DSN('redis://host:1234/dbname#connection_name')
        self.assertFalse("redis" in dsn.scheme)

    def test_password(self):
        dsn = DSN('redis://password@host:1234/dbname')
        self.assertTrue("password", dsn.password)

        dsn = DSN('redis://username:password@host:1234/dbname')
        self.assertTrue("password", dsn.password)

        dsn = DSN('redis://password/@host:1234/dbname')
        self.assertTrue("password/", dsn.password)


class ConfigureTest(TestCase):
    def test_configure(self):
        with self.assertRaises(KeyError):
            i = caches.get_interface('connection_name')

        dsn = 'caches.interface.Redis://host:1234/dbname#connection_name'
        caches.configure(dsn)
        i = caches.get_interface('connection_name')
        self.assertTrue(i)

    def test_configure_heroku_dsn(self):
        caches.interfaces = {}
        dsn = "caches.interface.Redis://redistogo:d381fd671fe61c0f6d36bdb4c25d3050@grideye.redistogo.com:10174/"
        caches.configure(dsn)

        i = caches.get_interface()
        self.assertTrue(i)

        i = caches.get_interface('')
        self.assertTrue(i)

    def test_defaults(self):
        dsn = DSN("redis://localhost?socket_timeout=1")
        self.assertEqual(6379, dsn.port)
        self.assertEqual(1.0, dsn.query["socket_timeout"])


