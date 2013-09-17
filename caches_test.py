from unittest import TestCase
import logging
import sys

import caches
from caches import Cache

# configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

class CacheTest(TestCase):

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
