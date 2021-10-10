# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import sys
import time
import random

from . import TestCase

import caches
from caches.compat import *
from caches.core import (
    Cache,
    DictCache,
    SetCache,
    SortedSetCache,
    SentinelCache,
)


class CacheTest(TestCase):
    def test_lifecycle(self):
        key = "c.lifecycle"
        c = Cache(key)
        self.assertIsNone(c.data)

        c.data = "boofar"
        self.assertIsNotNone(c.data)

        c2 = Cache(key)
        self.assertEqual(c.data, c2.data)

    def test_increment(self):
        c = Cache('ktest_increment')
        c.serialize = False
        c += 1

        self.assertEqual(1, c.data)

        c2 = Cache('ktest_increment')
        c2.serialize = False
        self.assertEqual(1, int(c2))

        c2.increment(10)
        self.assertEqual(11, int(c2))

        c2.increment(-5)
        self.assertEqual(6, int(c2))

        c3 = Cache('ktest_increment')
        c3.serialize = False
        self.assertEqual(6, int(c3))

    def test_key(self):
        c = Cache(['kfoo', 'bar'])
        self.assertEqual(None, c.data)

        c = Cache(['kfoo', 'bar'])
        self.assertFalse(c.has())
        c.data = "foo"
        self.assertTrue(c.has())

        self.assertEqual("foo", c.data)

        c = Cache(['kfoo', 'bar'])
        self.assertTrue(c.has())
        self.assertEqual("foo", c.data)

        c = Cache('kfoo')
        c.ttl = 1
        c.data = "boom"
        self.assertTrue(c.has())
        time.sleep(1)
        self.assertFalse(c.has())

        c = Cache('kfoo2')
        c.data = "boom2"
        self.assertTrue(c.has())
        self.assertEqual("boom2", c.data)

        delattr(c, 'data')
        self.assertFalse(c.has())

        self.assertEqual(None, c.data)

    def test_compare(self):
        c = Cache('KeyCache.__cmp__')

        self.assertFalse(c == "b")
        self.assertTrue(c < "c")
        self.assertFalse(c > "a")

        c.data = "b"
        c = Cache('KeyCache.__cmp__')
        self.assertTrue(c == "b")
        self.assertTrue(c < "c")
        self.assertTrue(c > "a")

    def test___int__(self):
        c = Cache('KeyCache.__int__')
        self.assertEqual(0, int(c))
        self.assertEqual(None, c.data)

        c.data = 5
        c = Cache('KeyCache.__int__')
        self.assertEqual(5, int(c))

    def test___bool__(self):
        c = Cache('KeyCache.__nonzero__')
        self.assertFalse(bool(c))

        c.data = 500
        c = Cache('KeyCache.__nonzero__')
        self.assertTrue(bool(c))

        c.data = 0
        self.assertFalse(bool(c))

    def test_normalize_data(self):

        class TKC(Cache):
            def normalize_data(self, val):
                if val is None: val = 0
                return int(val)

        c = Cache('KeyCache.normalize')
        self.assertFalse(isinstance(c.data, int))
        self.assertEqual(None, c.data)

        c = TKC('TKC.normalize')
        self.assertTrue(isinstance(c.data, int))
        self.assertEqual(0, c.data)

    def test_has(self):
        c = Cache('KeyCache.khas')
        self.assertFalse(c.has())

        c.data = "bah"
        self.assertTrue(c.has())

        c.clear()
        self.assertFalse(c.has())

    def test_cached(self):

        self.called_count = 0
        class TCached(Cache):
            prefix = 'Keycache.cached'

        @TCached.cached(key='cached')
        def calculate(x, y):
            self.called_count += 1
            return x + y

        self.assertEqual(0, self.called_count)
        r = calculate(1, 2)
        self.assertEqual(3, r)
        self.assertEqual(1, self.called_count)
        r = calculate(1, 2)
        self.assertEqual(3, r)
        self.assertEqual(1, self.called_count)

        c = TCached('cached')
        c.clear()

        r = calculate(1, 2)
        self.assertEqual(3, r)
        self.assertEqual(2, self.called_count)

        self.called_count = 0
        @TCached.cached(key=lambda *args: args)
        def calculate2(x, y):
            self.called_count += 1
            return x + y

        self.assertEqual(0, self.called_count)
        r = calculate2(1, 2)
        self.assertEqual(3, r)
        self.assertEqual(1, self.called_count)
        r = calculate2(1, 2)
        self.assertEqual(3, r)
        self.assertEqual(1, self.called_count)
        r = calculate2(3, 5)
        self.assertEqual(8, r)
        self.assertEqual(2, self.called_count)

    def test_duplicate_cached(self):
        class TDCached(Cache):
            prefix = 'Keycache.tdcached'
            ttl = 360

        @TDCached.cached(lambda x: str(x))
        def calculate(x):
            return x

        r = calculate("five")
        r = calculate("five")
        r = calculate("five")
        self.assertEqual("five", r)

    def test___del__(self):
        c = Cache('KeyCache.__del__')
        del(c.data)
        # if it doesn't raise an error, it worked correctly


class DictCacheTest(TestCase):
    def test___init__(self):
        key = ['dfoo__init__', 'bar__init__']

        dc = DictCache(key)
        self.assertFalse('foo' in dc)
        self.assertFalse('bar' in dc)

        dc = DictCache(key, data={'foo': 'foo', 'bar': 'bar'})
        self.assertTrue('foo' in dc)
        self.assertTrue('bar' in dc)

    def test___setitem__(self):
        td = {
            "foo": 1,
            "bar": 2,
        }
        d = DictCache("__setitem__")

        for k, v in td.items():
            d[k] = v

        self.assertEqual(len(td), len(d))
        for k, v in d.items():
            self.assertEqual(td[k], v)
        for k in d.keys():
            self.assertTrue(k in td)
        for v in d.values():
            self.assertTrue(v in set(td.values()))

    def test___getitem__(self):
        td = {
            "foo": 1,
            "bar": 2,
        }
        d = DictCache("__getitem__", data=td)

        for k, v in td.items():
            self.assertEqual(v, d[k])

    def test___delitem__(self):
        d = DictCache("__delitem__", data={"foo": 1})
        self.assertEqual(1, len(d))
        del d["foo"]
        self.assertEqual(0, len(d))

    def test_get(self):
        d = DictCache("get")

        with self.assertRaises(KeyError):
            d["foo"]

        self.assertIsNone(d.get("foo"))
        self.assertEqual(1, d.get("foo", 1))

        d["foo"] = 2
        self.assertEqual(2, d.get("foo", 1))

    def test_pop(self):
        d = DictCache("pop")

        with self.assertRaises(KeyError):
            d.pop("foo")

        self.assertIsNone(d.pop("foo", None))
        self.assertEqual(1, d.pop("foo", 1))

        d["foo"] = 2
        self.assertEqual(2, d.pop("foo", 1))
        self.assertFalse("foo" in d)

    def test_popitem(self):
        self.skip_test("This only works on Redis >=6.2")
        d = DictCache("popitem")

        with self.assertRaises(KeyError):
            d.popitem()

        d["foo"] = 2
        self.assertEqual(("foo", 2), d.popitem())
        self.assertFalse("foo" in d)

    def test___iter__(self):
        d = DictCache("__iter__", data={"foo": 1, "bar": 2, "che": 3})

        for count, k in enumerate(d, 1):
            pass
        self.assertEqual(len(d), count)

    def test_copy(self):
        d = DictCache("copy", data={"foo": 1, "bar": 2, "che": 3})
        d2 = d.copy()
        self.assertEqual(len(d), len(d2))
        d2.pop("foo")
        self.assertFalse("foo" in d2)
        self.assertTrue("foo" in d)

    def test_clear(self):
        d = DictCache("clear", data={"foo": 1, "bar": 2, "che": 3})
        self.assertLess(0, len(d))
        d.clear()
        self.assertEqual(0, len(d))

    def test_exists(self):
        c = DictCache('exists')
        self.assertFalse(c.exists())
        c['foo'] = 'foo'
        self.assertTrue(c.exists())

    def test___repr__(self):
        d = DictCache("clear", data={"foo": 1, "bar": 2, "che": 3})
        s = d.__repr__()
        self.assertTrue("foo" in s)
        self.assertTrue("bar" in s)
        self.assertTrue("che" in s)

    def test_dict(self):
        k = ['dfoo_set', 'bar_set']
        dc = DictCache(k, ttl=1)
        self.assertFalse('happy' in dc)
        dc['happy'] = 'sad'
        self.assertTrue('happy' in dc)
        time.sleep(1)
        self.assertFalse('happy' in dc)

        dc = DictCache(k + [2], data={'foo': 'foo', 'bar': 'bar'})
        self.assertTrue('foo' in dc)
        self.assertTrue('bar' in dc)
        self.assertEqual('foo', dc.get('foo'))
        self.assertEqual('bar', dc.get('bar'))
        self.assertEqual('foo', dc['foo'])
        self.assertEqual('bar', dc['bar'])

        d = {'che': 'che'}
        self.assertFalse('che' in dc)
        dc['che'] = d
        self.assertTrue('che' in dc)
        self.assertEqual(d, dc.get('che'))
        self.assertEqual(d, dc['che'])


class SetCacheTest(TestCase):
    def test___init__(self):
        # tests instantiation, __contains__, and update
        key = '__init__'

        s = SetCache(key)
        self.assertFalse('foo' in s)
        self.assertFalse('bar' in s)

        s = SetCache(key, data=['foo', 'bar'])
        self.assertTrue('foo' in s)
        self.assertTrue('bar' in s)

    def test_add(self):
        # tests add and __len__
        s = SetCache("add")
        self.assertEqual(0, len(s))

        s.add(1)
        self.assertEqual(1, len(s))
        self.assertTrue(1 in s)
        self.assertFalse(2 in s)

    def test_remove(self):
        # tests remove, and discard
        s = SetCache("remove", data="ABCDEFG")

        self.assertTrue("A" in s)
        s.remove("A")
        self.assertFalse("A" in s)

        with self.assertRaises(KeyError):
            s.remove("A")
        s.discard("A")

        self.assertTrue("B" in s)
        s.discard("B")
        self.assertFalse("B" in s)
        s.discard("B")

    def test_pop(self):
        # tests pop
        s = SetCache("pop")

        with self.assertRaises(KeyError):
            s.pop()

        s.update("ABC", "DEFG")
        self.assertTrue("D" in s)

        elem = s.pop()
        self.assertFalse(elem in s)
        self.assertTrue(elem in "ABCDEFG")

    def test___iter__(self):
        # tests __iter__
        data = "ABCDEFG"
        s = SetCache("__iter__", data=data)

        for count, elem in enumerate(data, 1):
            self.assertTrue(elem in data)
        self.assertLess(0, count)
        self.assertEqual(count, len(s))

    def test_copy(self):
        # tests copy
        data = "ABCDEFG"
        s = SetCache("copy", data=data)

        s2 = s.copy()
        self.assertEqual(len(s), len(s2))
        s2.remove("A")
        self.assertTrue("A" in s)
        self.assertFalse("A" in s2)

    def test___repr__(self):
        # test __repr__
        data = "ABCDEFG"
        s = SetCache("__repr__", data=data)

        sr = s.__repr__()
        for count, elem in enumerate(data, 1):
            self.assertTrue(elem in sr)
        self.assertLess(0, count)

    def test_clear(self):
        # tests clear
        data = "ABCDEFG"
        s = SetCache("clear", data=data)
        self.assertLess(0, len(s))

        s.clear()
        self.assertEqual(0, len(s))

    def test_exists(self):
        c = SetCache('exists')
        self.assertFalse(c.exists())
        c.add(1)
        self.assertTrue(c.exists())

    def test_set(self):
        key = ['sfoo__init__', 'bar__init__']
        c = SetCache(key, ttl=1)
        c.add('happy')
        self.assertTrue('happy' in c)
        time.sleep(1)
        self.assertFalse('happy' in c)

        c = SetCache(key, data=set([1, 2]))
        self.assertTrue(1 in c)
        self.assertTrue(2 in c)

        c.add(3)
        self.assertTrue(3 in c)


class SortedSetCacheTest(TestCase):
    def test___init__(self):
        # tests instantiation, __contains__, and update
        key = '__init__'

        s = SortedSetCache(key)
        self.assertFalse('foo' in s)
        self.assertFalse('bar' in s)

        with self.assertRaises(TypeError):
            s = SortedSetCache(key, data=['fo'])

        s = SortedSetCache(key, data=[(1, 'foo'), (2, 'bar')])

        self.assertTrue('foo' in s)
        self.assertTrue('bar' in s)

    def test_add(self):
        # tests add and __len__
        s = SortedSetCache("add")
        self.assertEqual(0, len(s))

        with self.assertRaises(TypeError):
            s.add("foo")

        with self.assertRaises(ValueError):
            s.add((1, 2, 3, "foo"))

        s.add((1, "foo"))
        self.assertEqual(1, len(s))
        self.assertTrue("foo" in s)
        self.assertFalse("bar" in s)

    def test_remove(self):
        # tests remove, and discard
        data = [(1, "A"), (2, "B"), (3, "C")]
        s = SortedSetCache("remove", data=data)

        self.assertTrue("A" in s)
        s.remove("A")
        self.assertFalse("A" in s)

        with self.assertRaises(KeyError):
            s.remove("A")
        s.discard("A")

        self.assertTrue("B" in s)
        s.discard("B")
        self.assertFalse("B" in s)
        s.discard("B")

    def test_pop(self):
        # tests pop and rpop
        s = SortedSetCache("pop")

        with self.assertRaises(KeyError):
            s.pop()

        data = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
        s.update(data)
        self.assertTrue("D" in s)

        score_asc, elem = s.pop()
        self.assertFalse(elem in s)

        score_desc, elem = s.rpop()
        self.assertFalse(elem in s)
        self.assertLess(score_asc, score_desc)

    def test_chunk(self):
        s = SortedSetCache("chunk")
        count = 1000
        for x in range(count):
            s.add((x + 1, x + 1))

        prev_score = 0
        for c, item in enumerate(s.chunk(chunk=500, desc=False), 1):
            self.assertLess(prev_score, item[0])
        self.assertEqual(count, c)

        prev_score = count + 1000
        for c, item in enumerate(s.chunk(chunk=500, desc=True), 1):
            self.assertGreater(prev_score, item[0])
        self.assertEqual(count, c)

    def test___iter__(self):
        # tests __iter__ and __reversed__
        data = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
        data_count = len(data)
        s = SortedSetCache("__iter__", data=data)

        prev_score = 0
        for count, item in enumerate(s, 1):
            self.assertTrue(item[1] in s)
            self.assertLess(prev_score, item[0])
        self.assertEqual(count, len(s))

        prev_score = data_count + 1000
        for count, item in enumerate(reversed(s), 1):
            self.assertTrue(item[1] in s)
            self.assertGreater(prev_score, item[0])
        self.assertEqual(count, len(s))

    def test_copy(self):
        # tests copy
        data = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
        s = SortedSetCache("copy", data=data)

        s2 = s.copy()
        self.assertEqual(len(s), len(s2))

    def test___repr__(self):
        # test __repr__
        data = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
        s = SortedSetCache("__repr__", data=data)

        sr = s.__repr__()
        for count, item in enumerate(data, 1):
            self.assertTrue(String(item) in sr)
        self.assertLess(0, count)

    def test_clear(self):
        # tests clear
        data = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
        s = SortedSetCache("clear", data=data)
        self.assertLess(0, len(s))
        s.clear()
        self.assertEqual(0, len(s))

    def test_exists(self):
        c = SortedSetCache('exists')
        self.assertFalse(c.exists())
        c.add((1, 1))
        self.assertTrue(c.exists())

    def test_queue(self):
        c = SortedSetCache('ssqueue', ttl=1)
        c.add((1, 'happy'))
        self.assertTrue('happy' in c)
        time.sleep(1)
        self.assertFalse('happy' in c)

        c = SortedSetCache('ssqueue', data=[(1, 1), (2, 2)])
        self.assertTrue(1 in c)
        self.assertTrue(2 in c)

        c.add((5, 3))
        self.assertTrue(3 in c)
        self.assertEqual((5, 3), c.pop(desc=True))
        self.assertEqual(2, len(c))


class SentinelCacheTest(TestCase):
    def test_check(self):
        s = SentinelCache("check")
        if s:
            raise ValueError("This should not have been raised")

        if not s:
            raise ValueError("This should not have been raised")


# class StateCacheData(object): pass
# class StateCacheTest(TestCase):
#     def test_instance_wrap(self):
# #         class Foo(object):
# #             def __getstate__(self):
# #                 pout.h()
# #                 return {"bar": self.bar, "che": self.che}
# 
#         f = ObjectCacheData()
#         f.bar = 1
#         f.che = 2
# 
#         fc = ObjectCache("oc.instance_wrap", f)
#         self.assertEqual(f.bar, fc.bar)
#         self.assertEqual(f.che, fc.che)
# 
#         self.assertTrue(isinstance(fc, ObjectCacheData))
# 
#         fc2 = ObjectCache("oc.instance_wrap")
#         self.assertEqual(f.bar, fc2.bar)
#         self.assertEqual(f.che, fc2.che)
#         self.assertTrue(isinstance(fc2, ObjectCacheData))
# 
#         fc2.baz = 3
#         self.assertEqual(3, fc2.baz)
# 
#         fc3 = ObjectCache("oc.instance_wrap")
#         self.assertEqual(fc2.baz, fc3.baz)
# 
#         del fc3.bar
#         with self.assertRaises(AttributeError):
#             fc3.bar
# 
#         fc4 = ObjectCache("oc.instance_wrap")
#         with self.assertRaises(AttributeError):
#             fc4.bar


