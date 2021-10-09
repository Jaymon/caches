# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from . import TestCase

from caches.compat import *
import caches
from caches.core import Cache


class CachesTest(TestCase):
    def test_clear_unsafe(self):
        Cache("foo", 1)
        Cache("bar", 1)
        Cache("foo.bar", 1)

        c = Cache("foo")
        self.assertTrue(c.exists())

        count = caches.clear_unsafe("*foo*")
        self.assertEqual(2, count)

        c = Cache("foo")
        self.assertFalse(c.exists())














# class DecoratorsTest(TestCase):
#     def test_keycache_decorator_classmethod(self):
#         class KCDCM(KeyCache):
#             prefix = 'kcdcm'
# 
#         class CachedKCDCM(object):
#             pk = 35
#             called = 0
#             @KCDCM.cached(key=lambda self: [self.pk])
#             def foo(self):
#                 self.called += 1
#                 return self.pk
# 
#         tf = CachedKCDCM()
#         self.assertEqual(tf.pk, tf.foo())
#         self.assertEqual(1, tf.called)
#         self.assertEqual(tf.pk, tf.foo())
#         self.assertEqual(1, tf.called)
# 
#     def test_method(self):
#         class CachedMethod(object):
#             pk = 25
#             called = 0
#             @cached(KeyCache, key=lambda self: [self.pk])
#             def foo(self):
#                 self.called += 1
#                 return self.pk
# 
#         tf = CachedMethod()
#         self.assertEqual(tf.pk, tf.foo())
#         self.assertEqual(1, tf.called)
#         self.assertEqual(tf.pk, tf.foo())
#         self.assertEqual(1, tf.called)
# 
#         tf.pk += 1
#         self.assertEqual(tf.pk, tf.foo())
#         self.assertEqual(2, tf.called)
#         self.assertEqual(tf.pk, tf.foo())
#         self.assertEqual(2, tf.called)
# 
#     def test_property(self):
#         class CachedProp(object):
#             pk = 15
#             called = 0
#             @property
#             @cached(KeyCache, key=lambda self: [self.pk])
#             def foo(self):
#                 self.called += 1
#                 return self.pk
# 
#         tf = CachedProp()
#         self.assertEqual(tf.pk, tf.foo)
#         self.assertEqual(1, tf.called)
#         self.assertEqual(tf.pk, tf.foo)
#         self.assertEqual(1, tf.called)
# 
#         tf.pk += 1
#         self.assertEqual(tf.pk, tf.foo)
#         self.assertEqual(2, tf.called)
#         self.assertEqual(tf.pk, tf.foo)
#         self.assertEqual(2, tf.called)
# 
#     def test_cached_classmethod(self):
#         class CMFoo(object):
#             pk = 5
# 
#         class CachedCM(object):
#             @classmethod
#             @cached(KeyCache, key=lambda cls, foo: [foo.pk])
#             def get_average(cls, foo):
#                 return 1.1
# 
#         tf = CMFoo()
#         tf.pk = 100
# 
#         r = CachedCM.get_average(tf)
#         self.assertEqual(1.1, r)
# 
#         r = CachedCM.get_average(tf)
#         self.assertEqual(1.1, r)
# 
#     def test_cached(self):
#         self.called = False
#         @cached(KeyCache, key=lambda *args, **kwargs: args)
#         def foo(*args):
#             self.called = True
#             return reduce(lambda x, y: x+y, args)
# 
#         self.called = False
#         v = foo(1, 2, 3)
#         self.assertTrue(self.called)
#         self.assertEqual(6, v)
# 
#         self.called = False
#         v = foo(1, 2, 3)
#         self.assertFalse(self.called)
#         self.assertEqual(6, v)
# 
#         self.called = False
#         v = foo(1, 2, 3, 4)
#         self.assertTrue(self.called)
#         self.assertEqual(10, v)
# 
#         self.called = False
#         v = foo(1, 2, 3, 4)
#         self.assertFalse(self.called)
#         self.assertEqual(10, v)
# 
#         self.called = False
#         v = foo(1, 2, 3)
#         self.assertFalse(self.called)
#         self.assertEqual(6, v)
# 
