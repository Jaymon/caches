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

import caches
from caches import DictCache, SetCache, KeyCache, SortedSetCache, CounterCache
from caches.collections import CountingSet, SortedSet

def setUpModule():
    """
    http://docs.python.org/2/library/unittest.html#setupmodule-and-teardownmodule
    """
    for interface_name, i in caches.interfaces.iteritems():
        i.flush()
        pass


class SortedSetTest(TestCase):

    def get_set(self, *args, **kwargs):
        r = caches.get_interface()
        kwargs['redis'] = r
        s = SortedSet(*args, **kwargs)
        return s

    def test_addnx(self):
        s = SortedSet()
        s.addnx('foo', 1)
        s.addnx('foo', 6)

        for r_elem, r_score in s:
            self.assertEqual(1, r_score)
            self.assertEqual('foo', r_elem)

        s.add('foo', 5)
        for r_elem, r_score in s:
            self.assertEqual(5, r_score)
            self.assertEqual('foo', r_elem)

        s.addnx('foo', 100)
        for r_elem, r_score in s:
            self.assertEqual(5, r_score)
            self.assertEqual('foo', r_elem)


class CountingSetTest(TestCase):

    def get_set(self, *args, **kwargs):
        r = caches.get_interface()
        kwargs['redis'] = r
        s = CountingSet(*args, **kwargs)
        return s

    def test_counting_set(self):
        data = range(1, 1002)
        s = self.get_set(data=data)
        count = 0
        for elem, rank in s:
            self.assertEqual(1, rank)
            count += 1

        self.assertEqual(count, len(data))

        s = self.get_set()
        self.assertEqual(0, len(s))
        s.add("foo")
        self.assertEqual(1, len(s))
        self.assertEqual(("foo", 1), s.pop())
        self.assertEqual(0, len(s))

        s = self.get_set()
        ints = range(1, 5)
        for x in xrange(10):
            i = random.choice(ints)
            s.add(i)

        count = len(s)
        highrank = 10
        for elem, rank in s:
            self.assertLessEqual(rank, highrank)
            highrank = rank
            count -= 1
        self.assertEqual(0, count)

        count = len(s)
        lowrank = 0
        for elem, rank in reversed(s):
            self.assertGreaterEqual(rank, lowrank)
            lowrank = rank
            count -= 1
        self.assertEqual(0, count)


class SortedSetCacheTest(TestCase):
    def test_queue(self):
        c = SortedSetCache('ssqueue')
        c.ttl = 1
        c.add('happy')
        self.assertTrue('happy' in c)
        time.sleep(1)
        self.assertFalse('happy' in c)

        c = SortedSetCache('ssqueue', data=set([1, 2]))
        self.assertTrue(1 in c)
        self.assertTrue(2 in c)

        c.add(3, 5)
        self.assertTrue(3 in c)
        self.assertEqual((3, 5), c.pop(last=True))
        self.assertEqual(2, len(c))


class KeyCacheTest(TestCase):
    def test_increment(self):
        c = KeyCache('ktest_increment')
        c.serialize = False
        c += 1

        self.assertEqual(1, c.data)

        c2 = KeyCache('ktest_increment')
        c2.serialize = False
        self.assertEqual(1, int(c2))

        c2.increment(10)
        self.assertEqual(11, int(c2))

        c2.increment(-5)
        self.assertEqual(6, int(c2))

        c3 = KeyCache('ktest_increment')
        c3.serialize = False
        self.assertEqual(6, int(c3))


    def test_key(self):
        c = KeyCache('kfoo', 'bar')
        self.assertFalse(c.has())
        c.data = "foo"
        self.assertTrue(c.has())

        self.assertEqual("foo", c.data)

        c = KeyCache('kfoo', 'bar')
        self.assertTrue(c.has())
        self.assertEqual("foo", c.data)


        c = KeyCache('kfoo')
        c.ttl = 1
        c.data = "boom"
        self.assertTrue(c.has())
        time.sleep(1)
        self.assertFalse(c.has())

        c = KeyCache('kfoo2')
        c.data = "boom2"
        self.assertTrue(c.has())
        self.assertEqual("boom2", c.data)

        delattr(c, 'data')
        self.assertFalse(c.has())

        #self.assertEqual('booyah', getattr(c, 'data', 'booyah'))
        self.assertEqual(None, c.data)

    def test_has(self):
        c = SetCache('khas')


class SetCacheTest(TestCase):
    def test_set(self):
        c = SetCache('sfoo__init__', 'bar__init__')
        c.ttl = 1
        c.add('happy')
        self.assertTrue('happy' in c)
        time.sleep(1)
        self.assertFalse('happy' in c)

        c = SetCache('sfoo__init__', 'bar__init__', data=set([1, 2]))
        self.assertTrue(1 in c)
        self.assertTrue(2 in c)

        c.add(3)
        self.assertTrue(3 in c)

    def test_has(self):
        c = SetCache('shas')
        self.assertFalse(c.has())
        c.add(1)
        self.assertTrue(c.has())


class DictCacheTest(TestCase):
    def test___init__(self):
        dc = DictCache('dfoo__init__', 'bar__init__')
        dc = DictCache('dfoo__init__', 'bar__init__', data={'foo': 'foo', 'bar': 'bar'})
        self.assertTrue('foo' in dc)
        self.assertTrue('bar' in dc)

    def test_dict(self):

        dc = DictCache('dfoo_set', 'bar_set')
        dc.ttl = 1
        self.assertFalse('happy' in dc)
        dc['happy'] = 'sad'
        self.assertTrue('happy' in dc)
        time.sleep(1)
        self.assertFalse('happy' in dc)

        dc = DictCache('dfoo_set', 'bar_set', 2, data={'foo': 'foo', 'bar': 'bar'})
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

    def test_has(self):
        c = DictCache('dhas')
        self.assertFalse(c.has())
        c['foo'] = 'foo'
        self.assertTrue(c.has())

class CounterTest(TestCase):
    def test_counter(self):
        cnt = CounterCache('test_counter')
        for word in ['red', 'blue', 'red', 'green', 'blue', 'blue']:
            cnt[word] += 1

        cnt2 = CounterCache('test_counter')
        self.assertEqual(3, cnt2['blue'])
        self.assertEqual(2, cnt2['red'])
        self.assertEqual(1, cnt2['green'])

        c = CounterCache('test_counter2')
        c.ttl = 1
        self.assertFalse('happy' in c)
        c['happy'] += 1
        self.assertTrue('happy' in c)
        time.sleep(1)
        self.assertFalse('happy' in c)


class CachesTest(TestCase):

    def test_configure(self):
        with self.assertRaises(KeyError):
            i = caches.get_interface('connection_name')

        dsn = 'caches.interface.Redis://host:1234/dbname#connection_name'
        caches.configure(dsn)
        i = caches.get_interface('connection_name')
        self.assertTrue(i)

