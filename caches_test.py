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
#from caches import Cache

from caches import DictCache, SetCache, KeyCache, PriorityQueueCache
from caches.collections import CountingSet

def setUpModule():
    """
    http://docs.python.org/2/library/unittest.html#setupmodule-and-teardownmodule
    """
    for interface_name, i in caches.interfaces.iteritems():
        i.flush()
        pass

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


class PriorityQueueCacheTest(TestCase):
    def test_queue(self):
        c = PriorityQueueCache('sfoo__init__', 'bar__init__')
        c.ttl = 1
        c.add('happy')
        self.assertTrue('happy' in c)
        time.sleep(1)
        self.assertFalse('happy' in c)

        c = PriorityQueueCache('sfoo__init__', 'bar__init__', data=set([1, 2]))
        self.assertTrue(1 in c)
        self.assertTrue(2 in c)

        c.add(3, 5)
        self.assertTrue(3 in c)
        self.assertEqual((3, 5), c.pop())
        self.assertEqual(2, len(c))


class KeyCacheTest(TestCase):
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


class CacheNone(TestCase):

    @classmethod
    def setUpClass(cls):
        """
        http://docs.python.org/2/library/unittest.html#unittest.TestCase.setUpClass
        """
        for interface_name, i in caches.interfaces.iteritems():
            i.flush()
            pass

    def test_configure(self):
        with self.assertRaises(KeyError):
            i = caches.get_interface('connection_name')

        dsn = 'caches.interface.redis.RedisInterface://host:1234/dbname#connection_name'
        caches.configure(dsn)
        i = caches.get_interface('connection_name')
        self.assertTrue(i)

    def test_create(self):
        c = Cache.create('foo4', 'bar4', val='boom4545')
        self.assertEqual(u'|1|foo4|bar4', c.key)
        self.assertEqual(u'boom4545', c.val)

    def test_add_key(self):
        c = Cache()
        c.add_key('foo', 'bar', val="boom!")
        self.assertEqual(u'|1|foo|bar', c.key)
        self.assertEqual(u'boom!', c.val)

        c = Cache()
        c.add_key('che')
        self.assertEqual(u'|1|che', c.key)
        self.assertEqual(None, c.val)


    def test_add_keys(self):
        c = Cache('foo', range(5), vals=range(100, 105))
        self.assertEqual(5, len(c.keys))
        self.assertEqual(u'|1|foo|0', c.key)
        self.assertEqual(100, c.val)

        with self.assertRaises(AssertionError):
            c = Cache('foo', range(2), range(5))

        c = Cache(1, xrange(5, 10), range(5))
        self.assertEqual(5, len(c.keys))

    def test_set_get_multi(self):
        c = Cache('just_x', xrange(5), vals=range(5))
        c.set()

        c = Cache('just_x', xrange(5))
        r = c.get()
        self.assertEqual(range(5), r)

    def test_set_get(self):
        c = Cache('foo', 'bar')
        self.assertEqual(u'|1|foo|bar', c.key)

        c.val = "this is the value"
        c.set()

        c2 = Cache('foo', 'bar')
        val = c2.get()
        self.assertEqual(c.val, c2.val)

    def test_increment(self):
        c = Cache('foo', val=1)
        c.json = False
        c.set()

        val = c.increment(2)
        self.assertEqual(3, val)

        c2 = Cache('foo')
        val = c2.get()
        self.assertEqual(3, val)

        c.val = -1
        c.increment()

        c2 = Cache('foo')
        val = c2.get()
        self.assertEqual(2, val)

    def test_delete(self):
        c = Cache('foo', val=1)
        c.set()

        c2 = Cache('foo')
        val = c2.get()
        self.assertEqual(1, val)

        c.delete()

        c2 = Cache('foo')
        val = c2.get()
        self.assertEqual(None, val)
