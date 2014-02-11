# stdlib
import importlib
import os
import json
import types
import itertools
import logging
from contextlib import contextmanager

try:
    import cPickle as pickle
except ImportError:
    import pickle

# 3rd party
import dsnparse

# first party
from redis_collections import Dict, Set, RedisCollection, Counter
from .collections import SortedSet
from . import decorators


__version__ = '0.2.11'

logger = logging.getLogger(__name__)

interfaces = {}
"""holds all the configured interfaces"""


def configure_environ(dsn_env_name='CACHES_DSN'):
    if dsn_env_name in os.environ:
        configure(os.environ[dsn_env_name])

    # now try importing 1 -> N dsns (eg CACHES_DSN_1, CACHES_DSN_2, ...)
    increment_name = lambda name, num: '{}_{}'.format(name, num)
    dsn_num = 1
    dsn_env_num_name = increment_name(dsn_env_name, dsn_num)
    if dsn_env_num_name in os.environ:
        try:
            while True:
                configure(os.environ[dsn_env_num_name])
                dsn_num += 1
                dsn_env_num_name = increment_name(dsn_env_name, dsn_num)

        except KeyError:
            pass


def configure(dsn):
    """
    configure an interface to be used to query a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    example dsn -- common.cache.RedisInterface://localhost:6379/0?timeout=5

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    global interfaces

    c = dsnparse.parse(dsn)
    assert c.fragment not in interfaces, 'a connection named "{}" has already been configured'.format(c.name)

    # compensate for passing pw as username (eg, not doing //:password@ but instead //password@)
    password = None
    if c.password:
        password = c.password
    elif c.username:
        password = c.username

    interface_module_name, interface_class_name = c.scheme.rsplit('.', 1)
    interface_module = importlib.import_module(interface_module_name)
    interface_class = getattr(interface_module, interface_class_name)

    i = interface_class(host=c.host, port=c.port, db=c.paths[0], password=password, **c.query)
    set_interface(i, c.fragment)
    return i


def set_interface(interface, name=''):
    """
    don't want to bother with a dsn? Use this method to make an interface available
    """
    assert interface, 'interface is empty'

    global interfaces
    logger.debug('connection_name: "{}" -> {}.{}'.format(name, interface.__module__, interface.__class__.__name__))
    interfaces[name] = interface


def get_interface(name=''):
    """
    get an interface that was created using configure()

    name -- string -- the name of the connection for the interface to return
    """
    global interfaces
    return interfaces[name]


class CacheError(Exception): pass


class Cache(object):
    """
    base caching class that all other caching classes inherit from, can't use on its own

    This is a mizin, it can't really be called on its own and is meant to be used
    with a child that also extends RedisCollection
    """

    serialize = True
    """true to make it pickle values"""

    prefix = ''
    """set the key prefix"""

    ttl = 0
    """how long to cache the result in seconds, 0 for unlimited"""

    connection_name = ''
    """the interface you want to use"""

    key_args = None

    def __init__(self, *args, **kwargs):
        self.key_args = args
        super(Cache, self).__init__(data=kwargs.get('data', None))

    @classmethod
    def create(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def has(self):
        """return True if the key exists in Redis"""
        return bool(self.redis.exists(self.key))

    def normalize(self, val):
        """this can be extended in child classes to further normalize the returned value,
        eg cast it as an int or something. This method is called from _unpickle()"""
        return val

    def _create_key(self):
        return u'.'.join(map(unicode, [self.prefix] + list(self.key_args)))

    def _pickle(self, val):
        if not self.serialize: return val
        return pickle.dumps(val, pickle.HIGHEST_PROTOCOL)

    def _unpickle(self, val):
        if val is None: return self.normalize(None)
        if not self.serialize: return self.normalize(val)
        if not isinstance(val, types.StringTypes):
            raise TypeError('Only strings can be unpickled (%r given).' % val)
        return self.normalize(pickle.loads(val))

    def _create_redis(self):
        return get_interface(self.connection_name)

    def _update(self, data, pipe=None):
        p = pipe
        exe = False
        if pipe is None and self.ttl:
            p = self.redis.pipeline()
            exe = True

        super(Cache, self)._update(data, pipe=p)

        if p:
            if self.ttl:
                p.expire(self.key, self.ttl)
            if exe:
                p.execute()


class SortedSetCache(Cache, SortedSet):
    """
    A sorted set cache that uses Redis' zset behing the scene and gives a very
    similar api similar to Python's built-in set
    """
    def add(self, elem, score=1):
        ret = False
        if self.ttl:
            with self.redis.pipeline() as pipe:
                self._add(elem, pipe=pipe, score=score)
                pipe.expire(self.key, self.ttl)
                ret = pipe.execute()[0]

        else:
            ret = super(SortedSetCache, self).add(elem, score)

        return bool(ret)

    def addnx(self, elem, score=1):
        ret = False
        if self.ttl:
            with self.redis.pipeline() as pipe:
                self._addnx(elem, pipe=pipe, score=score)
                pipe.expire(self.key, self.ttl)
                ret = pipe.execute()[0]

        else:
            ret = super(SortedSetCache, self).addnx(elem, score)

        return bool(ret)


class DictCache(Cache, Dict):
    """
    A Python dict but in Redis
    """
    def __setitem__(self, key, value):
        """Set ``d[key]`` to *value*."""
        value = self._pickle(value)
        if self.ttl:
            with self.redis.pipeline() as pipe:
                pipe.hset(self.key, key, value)
                pipe.expire(self.key, self.ttl)
                pipe.execute()

        else:
            p = self.redis.hset(self.key, key, value)

class SetCache(Cache, Set):
    """
    A Python set but in Redis
    """
    def add(self, elem):
        ret = False
        elem = self._pickle(elem)
        if self.ttl:
            with self.redis.pipeline() as pipe:
                pipe.sadd(self.key, elem)
                pipe.expire(self.key, self.ttl)
                ret = bool(pipe.execute()[0])

        else:
            ret = bool(self.redis.sadd(self.key, elem))

        return ret


class KeyCache(Cache, RedisCollection):
    """
    When you think of a traditional caching class, this is the class you most likely
    think of, this will cache a value at a key.

    example --
        c = KeyCache('foo', 'bar')
        c.data = "boom, this value is now cached" # cache the value

        c2 = KeyCache('foo', 'bar')
        print c2.data # "boom, this value is now cached"

        c = KeyCache('foo', 'count')
        c += 5
        print c.data # 5
        c+= 10
        print c.data # 15

        c2 = KeyCache('foo', 'count')
        print c2 # 15
    """
    @property
    def data(self):
        if not hasattr(self, '_d'):
            self._d = self._data()

        return self._d

    @data.setter
    def data(self, data):
        self._update(data)

    @data.deleter
    def data(self):
        self.clear()
        delattr(self, '_d')

    @classmethod
    def cached(cls, key=None, *args, **kwargs):
        """very similar to the generic decorator, except this one you don't have
        to specify the caching class, it will use the class whose method you called"""
        dec = decorators.cached(cls, key, *args, **kwargs)
        return dec

    def _data(self, pipe=None):
        self._d = self._unpickle(self.redis.get(self.key))
        return self._d

    def _update(self, data, pipe=None):
        assert not isinstance(data, RedisCollection), \
            "Not atomic. Use '_data()' within a transaction first."

        redis = pipe if pipe is not None else self.redis
        self._d = data
        data = self._pickle(data)

        if self.ttl:
            res = redis.setex(self.key, self.ttl, data)

        else:
            res = redis.set(self.key, data)

    def increment(self, delta):
        if self.serialize:
            raise ValueError("Cannot increment a serialized value")

        res = 0

        if self.ttl:
            with self.redis.pipeline() as pipe:
                pipe.incr(self.key, delta)
                pipe.expire(self.key, self.ttl)
                res = pipe.execute()[0]

        else:
            res = self.redis.incr(self.key, delta)

        res = int(res)
        self._d = res
        return res

    def __iadd__(self, other):
        self.increment(other)
        return self

    def __isub__(self, other):
        self.increment(-other)
        return self

    def __int__(self):
        v = self.data
        if not v: v = 0
        return int(v)

    def __long__(self):
        v = self.data
        if not v: v = 0
        return long(v)

    def __float__(self):
        v = self.data
        if not v: v = 0.0
        return float(v)

    def __str__(self):
        v = self.data
        if not v: v = ""
        return str(v)

    def __unicode__(self):
        v = self.data
        if not v: v = u""
        return unicode(v)

    def __cmp__(self, other):
        ret = 0
        v = self.data
        if v < other:
            ret = -1
        elif v > other:
            ret = 1

        return ret

    def __nonzero__(self):
        v = self.data
        return bool(v)

class CounterCache(Cache, Counter):
    """
    A Python collections.Counter instance, but in Redis

    http://docs.python.org/2/library/collections.html#collections.Counter
    """
    def __setitem__(self, key, value):
        """Set ``d[key]`` to *value*."""
        value = self._pickle(value)
        if self.ttl:
            with self.redis.pipeline() as pipe:
                pipe.hset(self.key, key, value)
                pipe.expire(self.key, self.ttl)
                pipe.execute()

        else:
            self.redis.hset(self.key, key, value)


configure_environ()

