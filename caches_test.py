from unittest import TestCase
import logging
import sys
import time
import random

# configure root logger before importing caches to make sure all logs print
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

# import caches
# from caches import DictCache, SetCache, KeyCache, SortedSetCache, CounterCache
# from caches.collections import CountingSet, SortedSet
# from caches.decorators import cached

import caches
from caches.core import Cache
from caches.datatypes import IntCache, StrCache, ObjectCache


def setUpModule():
    """
    http://docs.python.org/2/library/unittest.html#setupmodule-and-teardownmodule
    """
    for interface_name, i in caches.get_interfaces().items():
        i.flush()
        pass


class IntCacheTest(TestCase):
    def test_lifecycle(self):
        key = "ic.lifecycle"
        ic = IntCache(key)
        self.assertTrue(isinstance(ic, int))
        self.assertTrue(isinstance(ic, IntCache))
        self.assertEqual(0, ic)

        ic2 = ic + 4
        self.assertTrue(isinstance(ic2, int))
        self.assertFalse(isinstance(ic2, IntCache))

        ic2 = IntCache(key)
        ic2 += 4
        self.assertEqual(4, ic2)
        self.assertEqual(ic.key, ic2.key)

        ic2 += 5
        self.assertEqual(9, ic2)
        self.assertEqual(ic.key, ic2.key)

        ic3 = IntCache(key)
        self.assertEqual(ic2, ic3)

        ic3 -= 9
        self.assertEqual(ic, ic3)
        self.assertEqual(ic.key, ic3.key)

        ic4 = IntCache(key, data=5)
        self.assertEqual(5, ic4)

        ic4 *= 5
        self.assertEqual(25, ic4)

        ic4.clear()
        ic5 = IntCache(key)
        self.assertEqual(0, ic5)


class StrCacheTest(TestCase):
    def test_lifecycle(self):
        key = "sc.lifecycle"

        v = StrCache(key)
        self.assertEqual("", v)

        v += "foo"
        self.assertEqual("foo", v)

        v2 = v + "bar"
        self.assertTrue(isinstance(v2, str))
        self.assertFalse(isinstance(v2, StrCache))

        v3 = StrCache(key)
        self.assertEqual("foo", v3)



class ObjectCacheData(object): pass

class ObjectCacheTest(TestCase):
    def test_instance_wrap(self):
#         class Foo(object):
#             def __getstate__(self):
#                 pout.h()
#                 return {"bar": self.bar, "che": self.che}

        f = ObjectCacheData()
        f.bar = 1
        f.che = 2

        fc = ObjectCache("oc.instance_wrap", f)
        self.assertEqual(f.bar, fc.bar)
        self.assertEqual(f.che, fc.che)

        self.assertTrue(isinstance(fc, ObjectCacheData))

        fc2 = ObjectCache("oc.instance_wrap")
        self.assertEqual(f.bar, fc2.bar)
        self.assertEqual(f.che, fc2.che)
        self.assertTrue(isinstance(fc2, ObjectCacheData))

        fc2.baz = 3
        self.assertEqual(3, fc2.baz)

        fc3 = ObjectCache("oc.instance_wrap")
        self.assertEqual(fc2.baz, fc3.baz)

        del fc3.bar
        with self.assertRaises(AttributeError):
            fc3.bar

        fc4 = ObjectCache("oc.instance_wrap")
        with self.assertRaises(AttributeError):
            fc4.bar


class CacheTest(TestCase):
    def test_lifecycle(self):
        key = "c.lifecycle"
        c = Cache(key)
        self.assertIsNone(c.data)

        c.data = "boofar"
        self.assertIsNotNone(c.data)

        c2 = Cache(key)
        self.assertEqual(c.data, c2.data)

















# class SortedSetTest(TestCase):
# 
#     def get_set(self, *args, **kwargs):
#         r = caches.get_interface()
#         kwargs['redis'] = r
#         s = SortedSet(*args, **kwargs)
#         return s
# 
#     def test_addnx(self):
#         s = self.get_set()
#         s.addnx('foo', 1)
#         s.addnx('foo', 6)
# 
#         for r_elem, r_score in s:
#             self.assertEqual(1, r_score)
#             self.assertEqual('foo', r_elem)
# 
#         s.add('foo', 5)
#         for r_elem, r_score in s:
#             self.assertEqual(5, r_score)
#             self.assertEqual('foo', r_elem)
# 
#         s.addnx('foo', 100)
#         for r_elem, r_score in s:
#             self.assertEqual(5, r_score)
#             self.assertEqual('foo', r_elem)
# 
#         s = SortedSet()
#         s.addnx('bar', 0)
#         s.addnx('bar', 1)
#         for r_elem, r_score in s:
#             self.assertEqual(0, r_score)
#             self.assertEqual('bar', r_elem)
# 
#     def test_iter(self):
#         test_s = set()
#         total = 502
#         s = self.get_set()
#         for x in range(total):
#             v = 'x{}'.format(x)
#             s.add(v, x)
#             test_s.add(v)
# 
#         self.assertEqual(total, len(s))
# 
#         count = 0
#         for elem, rank in s:
#             count += 1
#             test_s.remove(elem)
#         self.assertEqual(total, count)
# 
# 
# class CountingSetTest(TestCase):
# 
#     def get_set(self, *args, **kwargs):
#         r = caches.get_interface()
#         kwargs['redis'] = r
#         s = CountingSet(*args, **kwargs)
#         return s
# 
#     def test_counting_set(self):
#         data = range(1, 1002)
#         s = self.get_set(data=data)
#         count = 0
#         for elem, rank in s:
#             self.assertEqual(1, rank)
#             count += 1
# 
#         self.assertEqual(count, len(data))
# 
#         s = self.get_set()
#         self.assertEqual(0, len(s))
#         s.add("foo")
#         self.assertEqual(1, len(s))
#         self.assertEqual(("foo", 1), s.pop())
#         self.assertEqual(0, len(s))
# 
#         s = self.get_set()
#         ints = range(1, 5)
#         for x in xrange(10):
#             i = random.choice(ints)
#             s.add(i)
# 
#         count = len(s)
#         highrank = 10
#         for elem, rank in s:
#             self.assertLessEqual(rank, highrank)
#             highrank = rank
#             count -= 1
#         self.assertEqual(0, count)
# 
#         count = len(s)
#         lowrank = 0
#         for elem, rank in reversed(s):
#             self.assertGreaterEqual(rank, lowrank)
#             lowrank = rank
#             count -= 1
#         self.assertEqual(0, count)
# 
# 
# class SortedSetCacheTest(TestCase):
#     def test_queue(self):
#         c = SortedSetCache('ssqueue')
#         c.ttl = 1
#         c.add('happy')
#         self.assertTrue('happy' in c)
#         time.sleep(1)
#         self.assertFalse('happy' in c)
# 
#         c = SortedSetCache('ssqueue', data=set([1, 2]))
#         self.assertTrue(1 in c)
#         self.assertTrue(2 in c)
# 
#         c.add(3, 5)
#         self.assertTrue(3 in c)
#         self.assertEqual((3, 5), c.pop(last=True))
#         self.assertEqual(2, len(c))
# 
# 
# class KeyCacheTest(TestCase):
#     def test_increment(self):
#         c = KeyCache('ktest_increment')
#         c.serialize = False
#         c += 1
# 
#         self.assertEqual(1, c.data)
# 
#         c2 = KeyCache('ktest_increment')
#         c2.serialize = False
#         self.assertEqual(1, int(c2))
# 
#         c2.increment(10)
#         self.assertEqual(11, int(c2))
# 
#         c2.increment(-5)
#         self.assertEqual(6, int(c2))
# 
#         c3 = KeyCache('ktest_increment')
#         c3.serialize = False
#         self.assertEqual(6, int(c3))
# 
# 
#     def test_key(self):
#         c = KeyCache('kfoo', 'bar')
#         self.assertEqual(None, c.data)
# 
#         c = KeyCache('kfoo', 'bar')
#         self.assertFalse(c.has())
#         c.data = "foo"
#         self.assertTrue(c.has())
# 
#         self.assertEqual("foo", c.data)
# 
#         c = KeyCache('kfoo', 'bar')
#         self.assertTrue(c.has())
#         self.assertEqual("foo", c.data)
# 
#         c = KeyCache('kfoo')
#         c.ttl = 1
#         c.data = "boom"
#         self.assertTrue(c.has())
#         time.sleep(1)
#         self.assertFalse(c.has())
# 
#         c = KeyCache('kfoo2')
#         c.data = "boom2"
#         self.assertTrue(c.has())
#         self.assertEqual("boom2", c.data)
# 
#         delattr(c, 'data')
#         self.assertFalse(c.has())
# 
#         #self.assertEqual('booyah', getattr(c, 'data', 'booyah'))
#         self.assertEqual(None, c.data)
# 
#     def test___cmp__(self):
#         c = KeyCache('KeyCache.__cmp__')
# 
#         self.assertFalse(c == "b")
#         self.assertTrue(c < "c")
#         self.assertFalse(c > "a")
# 
#         c.data = "b"
#         c = KeyCache('KeyCache.__cmp__')
#         self.assertTrue(c == "b")
#         self.assertTrue(c < "c")
#         self.assertTrue(c > "a")
# 
#     def test___int__(self):
#         c = KeyCache('KeyCache.__int__')
#         self.assertEqual(0, int(c))
#         self.assertEqual(None, c.data)
# 
#         c.data = 5
#         c = KeyCache('KeyCache.__int__')
#         self.assertEqual(5, int(c))
# 
#     def test___nonzero__(self):
#         c = KeyCache('KeyCache.__nonzero__')
#         self.assertFalse(bool(c))
# 
#         c.data = 500
#         c = KeyCache('KeyCache.__nonzero__')
#         self.assertTrue(bool(c))
# 
#         c.data = 0
#         self.assertFalse(bool(c))
# 
#     def test_normalize(self):
# 
#         class TKC(KeyCache):
#             def normalize(self, val):
#                 if val is None: val = 0
#                 return int(val)
# 
#         c = KeyCache('KeyCache.normalize')
#         self.assertFalse(isinstance(c.data, int))
#         self.assertEqual(None, c.data)
# 
#         c = TKC('TKC.normalize')
#         self.assertTrue(isinstance(c.data, int))
#         self.assertEqual(0, c.data)
# 
#     def test_has(self):
#         c = KeyCache('KeyCache.khas')
#         self.assertFalse(c.has())
# 
#         c.data = "bah"
#         self.assertTrue(c.has())
# 
#         c.clear()
#         self.assertFalse(c.has())
# 
#     def test_cached(self):
# 
#         self.called_count = 0
#         class TCached(KeyCache):
#             prefix = 'Keycache.cached'
# 
#         @TCached.cached(key='cached')
#         def calculate(x, y):
#             self.called_count += 1
#             return x + y
# 
#         self.assertEqual(0, self.called_count)
#         r = calculate(1, 2)
#         self.assertEqual(3, r)
#         self.assertEqual(1, self.called_count)
#         r = calculate(1, 2)
#         self.assertEqual(3, r)
#         self.assertEqual(1, self.called_count)
# 
#         c = TCached('cached')
#         c.clear()
# 
#         r = calculate(1, 2)
#         self.assertEqual(3, r)
#         self.assertEqual(2, self.called_count)
# 
#         self.called_count = 0
#         @TCached.cached(key=lambda *args: args)
#         def calculate2(x, y):
#             self.called_count += 1
#             return x + y
# 
#         self.assertEqual(0, self.called_count)
#         r = calculate2(1, 2)
#         self.assertEqual(3, r)
#         self.assertEqual(1, self.called_count)
#         r = calculate2(1, 2)
#         self.assertEqual(3, r)
#         self.assertEqual(1, self.called_count)
#         r = calculate2(3, 5)
#         self.assertEqual(8, r)
#         self.assertEqual(2, self.called_count)
# 
#     def test_duplicate_cached(self):
#         class TDCached(KeyCache):
#             prefix = 'Keycache.tdcached'
#             ttl = 360
# 
#         @TDCached.cached(lambda x: str(x))
#         def calculate(x):
#             return x
# 
#         r = calculate("five")
#         r = calculate("five")
#         r = calculate("five")
#         self.assertEqual("five", r)
# 
#     def test___del__(self):
#         c = KeyCache('KeyCache.__del__')
#         del(c.data)
#         # if it doesn't raise an error, it worked correctly
# 
# 
# class SetCacheTest(TestCase):
#     def test_set(self):
#         c = SetCache('sfoo__init__', 'bar__init__')
#         c.ttl = 1
#         c.add('happy')
#         self.assertTrue('happy' in c)
#         time.sleep(1)
#         self.assertFalse('happy' in c)
# 
#         c = SetCache('sfoo__init__', 'bar__init__', data=set([1, 2]))
#         self.assertTrue(1 in c)
#         self.assertTrue(2 in c)
# 
#         c.add(3)
#         self.assertTrue(3 in c)
# 
#     def test_has(self):
#         c = SetCache('shas')
#         self.assertFalse(c.has())
#         c.add(1)
#         self.assertTrue(c.has())
# 
# 
# class DictCacheTest(TestCase):
#     def test___init__(self):
#         dc = DictCache('dfoo__init__', 'bar__init__')
#         dc = DictCache('dfoo__init__', 'bar__init__', data={'foo': 'foo', 'bar': 'bar'})
#         self.assertTrue('foo' in dc)
#         self.assertTrue('bar' in dc)
# 
#     def test_dict(self):
# 
#         dc = DictCache('dfoo_set', 'bar_set')
#         dc.ttl = 1
#         self.assertFalse('happy' in dc)
#         dc['happy'] = 'sad'
#         self.assertTrue('happy' in dc)
#         time.sleep(1)
#         self.assertFalse('happy' in dc)
# 
#         dc = DictCache('dfoo_set', 'bar_set', 2, data={'foo': 'foo', 'bar': 'bar'})
#         self.assertTrue('foo' in dc)
#         self.assertTrue('bar' in dc)
#         self.assertEqual('foo', dc.get('foo'))
#         self.assertEqual('bar', dc.get('bar'))
#         self.assertEqual('foo', dc['foo'])
#         self.assertEqual('bar', dc['bar'])
# 
#         d = {'che': 'che'}
#         self.assertFalse('che' in dc)
#         dc['che'] = d
#         self.assertTrue('che' in dc)
#         self.assertEqual(d, dc.get('che'))
#         self.assertEqual(d, dc['che'])
# 
#     def test_has(self):
#         c = DictCache('dhas')
#         self.assertFalse(c.has())
#         c['foo'] = 'foo'
#         self.assertTrue(c.has())
# 
# class CounterTest(TestCase):
#     def test_counter(self):
#         cnt = CounterCache('test_counter')
#         for word in ['red', 'blue', 'red', 'green', 'blue', 'blue']:
#             cnt[word] += 1
# 
#         cnt2 = CounterCache('test_counter')
#         self.assertEqual(3, cnt2['blue'])
#         self.assertEqual(2, cnt2['red'])
#         self.assertEqual(1, cnt2['green'])
# 
#         c = CounterCache('test_counter2')
#         c.ttl = 1
#         self.assertFalse('happy' in c)
#         c['happy'] += 1
#         self.assertTrue('happy' in c)
#         time.sleep(1)
#         self.assertFalse('happy' in c)
# 
# 
# class CachesTest(TestCase):
#     def test_configure(self):
#         with self.assertRaises(KeyError):
#             i = caches.get_interface('connection_name')
# 
#         dsn = 'caches.interface.Redis://host:1234/dbname#connection_name'
#         caches.configure(dsn)
#         i = caches.get_interface('connection_name')
#         self.assertTrue(i)
# 
#     def test_configure_heroku_dsn(self):
#         interfaces = caches.interfaces
# 
#         caches.interfaces = {}
#         dsn = "caches.interface.Redis://redistogo:d381fd671fe61c0f6d36bdb4c25d3050@grideye.redistogo.com:10174/"
#         caches.configure(dsn)
# 
#         i = caches.get_interface()
#         self.assertTrue(i)
# 
#         i = caches.get_interface('')
#         self.assertTrue(i)
# 
#         caches.interfaces = interfaces
# 
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
