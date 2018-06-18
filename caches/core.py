# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import itertools
try:
    import cPickle as pickle
except ImportError:
    import pickle

from .decorators import classproperty, cached
from .interface import get_interface



class BaseCache(object):
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

    default = None
    """if no value is found in the cache, return this value"""

    @classproperty
    def interface(cls):
        """
        return an Interface instance that can be used to access the db
        return -- Interface() -- the interface instance this Orm will use
        """
        return get_interface(cls.connection_name)

    @classmethod
    def cached(cls, *args, **kwargs):
        """very similar to the generic decorator, except this one you don't have
        to specify the caching class, it will use the class whose method you called"""
        dec = cached(cls, *args, **kwargs)
        return dec


class Cache(BaseCache):
    """
    When you think of a traditional caching class, this is the class you most likely
    think of, this will cache a value at a key.

    example --
        c = Cache(['foo', 'bar'])
        c.data = "boom, this value is now cached" # cache the value

        c2 = KeyCache(['foo', 'bar'])
        print c2.data # "boom, this value is now cached"
    """
    @property
    def data(self):
        if not hasattr(self, '_data'):
            key = self.key
            data = self.interface.get(key)
            if data is None:
                data = self.default
            else:
                data = self.from_interface(data)
            self._data = data

        return self._data

    @data.setter
    def data(self, data):
        #self.__class__ = data.__class__
        self._data = data
        data = self.to_interface(data)
        ttl = self.ttl
        key = self.key
        if ttl:
            res = self.interface.setex(key, ttl, data)
        else:
            res = self.interface.set(key, data)

    @data.deleter
    def data(self):
        key = self.key
        self.interface.delete(key)
        try:
            delattr(self, '_data')
        except AttributeError: pass

    def __init__(self, key, data=None, **kwargs):
        #self.__origclass__ = self.__class__
        self.key = self.create_key(key)

        if data:
            self.data = data

        if "val" in kwargs:
            self._data = kwargs.pop("val")

        # allow for overriding class value with passed in values
        for k, v in kwargs.items():
            setattr(self, k, v)

    def clear(self):
        del self.data

    def has(self):
        """return True if the key exists in Redis"""
        return bool(self.redis.exists(self.key))

    def create_key(self, key):
        if isinstance(key, basestring):
            key = [key]
        return '.'.join(map(str, itertools.chain([self.prefix] if self.prefix else [], key)))

    def to_interface(self, val):
        if val is None or not self.serialize: return val
        return pickle.dumps(val, pickle.HIGHEST_PROTOCOL)

    def from_interface(self, val):
        if val is None or not self.serialize: return val
        if not isinstance(val, basestring):
            raise TypeError('Only strings can be unpickled (%r given).' % val)
        return pickle.loads(val)









class KeyCache(Cache):
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
            self._d = self._unpickle(self.interface.get(self.key))

        return self._d

    @data.setter
    def data(self, data):
        self._d = data
        data = self._to_interface(data)
        if self.ttl:
            res = self.interface.setex(self.key, self.ttl, data)

        else:
            res = self.interface.set(self.key, data)


    @data.deleter
    def data(self):
        self.interface.delete(self.key)
        try:
            delattr(self, '_d')
        except AttributeError: pass

    def increment(self, delta):
        if self.serialize:
            raise ValueError("Cannot increment a serialized value")

        res = 0

        if self.ttl:
            with self.interface.pipeline() as pipe:
                pipe.incr(self.key, delta)
                pipe.expire(self.key, self.ttl)
                res = pipe.execute()[0]

        else:
            res = self.interface.incr(self.key, delta)

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


