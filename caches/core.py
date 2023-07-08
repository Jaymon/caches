# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import itertools
from contextlib import contextmanager

from .compat import *
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

    def __init__(self, key, data=None, **kwargs):
        # allow for overriding class value with passed in values
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.key = self.normalize_key(key)
        self.update(data)

    def normalize_key(self, key):
        if isinstance(key, (basestring, int)):
            key = [key]
        prefixes = [self.normalize_prefix(self.prefix)]
        return '.'.join(map(String, itertools.chain(prefixes, key)))

    def normalize_data(self, data):
        return data

    def normalize_ttl(self, ttl):
        return int(self.ttl)

    def normalize_prefix(self, prefix):
        if not prefix:
            prefix = self.__class__.__name__
        return prefix

    def to_interface(self, val):
        if val is None or not self.serialize: return val
        return pickle.dumps(val, pickle.HIGHEST_PROTOCOL)

    def from_interface(self, val):
        if val is None or not self.serialize: return val
        if not isinstance(val, basestring):
            raise TypeError('Only strings can be unpickled (%r given).' % val)
        return pickle.loads(val)

    def exists(self):
        """return True if the key exists in Redis"""
        return bool(self.interface.exists(self.key))

    def has(self):
        return self.exists()

    def clear(self):
        self.interface.delete(self.key)

    def update(self, data):
        raise NotImplementedError()

    @contextmanager
    def pipeline(self, **kwargs):
        with self.interface.pipeline() as pipe:
            yield pipe
            r = pipe.execute()

#         pipeinfo = getattr(self, "_pipeinfo", {"counter": 0, "pipe": None})
#         pipeinfo["counter"] += 1
#         if not pipeinfo["pipe"]:
#             pipeinfo["pipe"] = self.interface.pipeline()
#             self._pipeinfo = pipeinfo
# 
#         yield pipeinfo["pipe"]
#         pipeinfo["counter"] -= 1
# 
#         if pipeinfo["counter"] <= 0:
#             pipeinfo["pipe"].execute()
#             del self._pipeinfo


class Cache(BaseCache):
    """
    When you think of a traditional caching class, this is the class you most likely
    think of, this will cache a value at a key.

    :example:
        c = Cache('foo', 'bar')
        c.data = "boom, this value is now cached" # cache the value

        c2 = Cache(['foo', 'bar'])
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
            self._data = self.normalize_data(data)
        return self._data

    @data.setter
    def data(self, data):
        self._data = self.normalize_data(data)
        data = self.to_interface(self._data)
        key = self.key
        if self.ttl:
            res = self.interface.setex(key, self.normalize_ttl(self.ttl), data)
        else:
            res = self.interface.set(key, data)

    @data.deleter
    def data(self):
        key = self.key
        self.interface.delete(key)
        try:
            delattr(self, '_data')
        except AttributeError: pass

    def update(self, data):
        if data is not None:
            self.data = data

    def clear(self):
        del self.data

    def increment(self, delta):
        if self.serialize:
            raise ValueError("Cannot increment a serialized value")

        res = 0
        with self.interface.pipeline() as pipe:
            pipe.incr(self.key, delta)
            if self.ttl:
                pipe.expire(self.key, self.normalize_ttl(self.ttl))
            res = pipe.execute()[0]

        res = int(res)
        self._data = res
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
        return String(v)

    def __eq__(self, other):
        """Defines behavior for the equality operator, ==."""
        return self.data == other

    def __ne__(self, other):
        """Defines behavior for the inequality operator, !=."""
        return not self.__eq__(other)

    def __lt__(self, other):
        """Defines behavior for the less-than operator, <."""
        if self.data is None and other is not None:
            return True
        return self.data < other

    def __gt__(self, other):
        """Defines behavior for the greater-than operator, >."""
        if self.data is None and other is not None:
            return False
        return self.data > other

    def __le__(self, other):
        """Defines behavior for the less-than-or-equal-to operator, <=."""
        if self.data is None and other is not None:
            return False
        return self.data <= other

    def __ge__(self, other):
        """Defines behavior for the greater-than-or-equal-to operator, >="""
        if self.data is None and other is not None:
            return False
        return self.data >= other

    def __bool__(self):
        v = self.data
        return bool(v)


class DictCache(BaseCache, dict):
    """A Python dict but in Redis

    https://redis.io/commands#hash
    """
    def update(self, data):
        # create temp dictionary so I don't have to mess with the arguments
        d = dict(data or {})
        for k, v in d.items():
            self[k] = v

    def __setitem__(self, k, v):
        """Set self[k] to v"""
        data = self.to_interface(self.normalize_data(v))
        with self.pipeline() as pipe:
            pipe.hset(self.key, k, data)
            if self.ttl:
                pipe.expire(self.key, self.normalize_ttl(self.ttl))

    def __getitem__(self, k):
        data = self.interface.hget(self.key, k)
        if data is None:
            if k not in self:
                raise KeyError(k)
        return self.normalize_data(self.from_interface(data))

    def __delitem__(self, k):
        return self.interface.hdel(self.key, k)

    def __contains__(self, k):
        return self.interface.hexists(self.key, k)

    def __len__(self):
        return self.interface.hlen(self.key)

    def __iter__(self):
        for k in self.keys():
            yield k

    def __repr__(self):
        return self.copy().__repr__()

    def keys(self):
        for k in self.interface.hkeys(self.key):
            yield String(k)

    def items(self):
        d = self.interface.hgetall(self.key) or {}
        for k, data in d.items():
            v = self.normalize_data(self.from_interface(data))
            yield String(k), v

    def values(self):
        for k, v in self.items():
            yield v

    def get(self, k, default=None):
        try:
            v = self[k]
        except KeyError:
            v = default
        return v

    def setdefault(self, k, default=None):
        if k not in self:
            self[k] = default

    def pop(self, k, *default):
        try:
            with self.pipeline() as pipe:
                pipe.hget(self.key, k)
                pipe.hdel(self.key, k)
                ret = pipe.execute()
                data = ret[0]
                if data is None:
                    if ret[1] == 0:
                        raise KeyError(k)

                v = self.normalize_data(self.from_interface(data))

        except KeyError:
            if default:
                v = default[0]
            else:
                raise
        return v

    def popitem(self):
        """unlike the actual dict.popitem of python >=3.7, this pops a random
        item

        https://docs.python.org/3/library/stdtypes.html#dict.popitem
        """
        k = self.interface.hrandfield(self.key, 1)
        if k:
            k = k[0]
            v = self.pop(k)
            return (String(k), v)

        else:
            raise KeyError()

    def copy(self):
        """Return a local copy divorced from the backend interface"""
        return dict(self)


class SetCache(BaseCache, set):
    """Membership set backed by redis

    https://docs.python.org/3/library/stdtypes.html#set
    https://redis.io/commands#set
    """
    def add(self, elem, **kwargs):
        data = self.to_interface(self.normalize_data(elem))
        with self.pipeline() as pipe:
            self._add(elem, pipe, **kwargs)

    def _add(self, elem, pipe, **kwargs):
        data = self.to_interface(self.normalize_data(elem))
        pipe.sadd(self.key, data)
        if self.ttl:
            pipe.expire(self.key, self.normalize_ttl(self.ttl))

    def update(self, *data):
        with self.pipeline() as pipe:
            for iterator in data:
                if iterator:
                    for elem in iterator:
                        self._add(elem, pipe)

    def remove(self, elem):
        """Remove element elem from the set. Raises KeyError if elem is not contained in the set."""
        data = self.to_interface(self.normalize_data(elem))
        res = self.interface.srem(self.key, data) # 1 (success) or 0 (failure)
        if not res:
            raise KeyError(elem)

    def discard(self, elem):
        """Remove element elem from the set if it is present."""
        try:
            self.remove(elem)
        except KeyError:
            pass

    def pop(self):
        """Remove and return an arbitrary element from the set. Raises KeyError if the set is empty."""
        data = self.interface.spop(self.key, 1) # returns list
        if not data:
            raise KeyError()

        elem = self.normalize_data(self.from_interface(data[0]))
        return elem

    def __len__(self):
        return int(self.interface.scard(self.key))

    def __contains__(self, elem):
        """Test for membership of *elem* in the set"""
        data = self.to_interface(self.normalize_data(elem))
        rank = self.interface.sismember(self.key, data)
        return bool(rank)

    def __iter__(self):
        for data in self.interface.smembers(self.key):
            yield self.normalize_data(self.from_interface(data))

    def copy(self):
        """Return a local copy divorced from the backend interface"""
        return set(self.__iter__())

    def __repr__(self):
        return self.copy().__repr__()

    def noimp(self, *args, **kwargs):
        raise NotImplementedError()

    __sub__ = noimp
    __and__ = noimp
    __or__ = noimp
    __xor__ = noimp
    __isub__ = noimp
    __iand__ = noimp
    __ior__ = noimp
    __ixor__ = noimp
    intersection_update = noimp # https://redis.io/commands/sinter
    difference_update = noimp # https://redis.io/commands/sdiff
    symmetric_difference_update = noimp
    symmetric_difference = noimp
    difference = noimp # https://redis.io/commands/sdiff
    intersection = noimp # https://redis.io/commands/sinter
    union = noimp # https://redis.io/commands/sunion


class SortedSetCache(SetCache):
    """
    A sorted set cache that uses Redis' zset behind the scene and gives a very
    similar api similar to Python's built-in set

    Sorted set uses a similar api to Python's built in set but offers the ability
    to add a rank to each element in order to sort the set

    Changes from standard Python set:
        * add() -- required to take a tuple (score, elem)
        * pop() -- retuns a tuple (score, elem) while a normal set would just return elem
        * __iter__ -- returns tuples like pop()

    https://redis.io/commands#sorted_set
    http://docs.python.org/2/library/stdtypes.html#set
    http://code.activestate.com/recipes/576694/
    """
    def normalize_score(self, score):
        """normalize the score, this method is called anytime score is added/retrieved"""
        return int(score)

    def _add(self, item: tuple, pipe, **kwargs):
        """add an item tuple of (score, elem) to the set

        https://redis.io/commands/zadd

        :param item: tuple (score, elem)
        :param **kwargs: these are passed to the interface
            xx -- bool, only update elements that already exist. Don't add new elements
            nx -- bool, only add new elements. Don't update aleady existing elements
            lt -- bool, only update existing elem if new score is less than current score
            gt -- bool, only update existing elem if new score is greater than current score
        """
        if isinstance(item, tuple):
            if len(item) != 2:
                raise ValueError(
                    " ".join([
                        "SortedSetCache only accepts (score, elem) tuples,",
                        "got tuple with {} values".format(
                            len(item),
                        )
                    ])
                )

        else:
            raise TypeError(
                "SortedSetCache only accepts (score, elem) tuples, got {}".format(
                    type(item),
                )
            )

        score, elem = item
        args = [self.key]
        score = self.normalize_score(score)
        data = self.to_interface(self.normalize_data(elem))
        # https://github.com/andymccurdy/redis-py/issues/625
        # https://github.com/redis/redis/pull/1132
        # https://github.com/redis/redis/issues/1128
        # https://redis.io/commands/zadd
        pipe.zadd(self.key, {data: score}, **kwargs)
        if self.ttl:
            pipe.expire(self.key, self.normalize_ttl(self.ttl))

    def remove(self, elem):
        """Remove element elem from the set. Raises KeyError if elem is not contained in the set."""
        data = self.to_interface(self.normalize_data(elem))
        res = self.interface.zrem(self.key, data) # 1 (success) or 0 (failure)
        if not res:
            raise KeyError(elem)

    def pop(self, desc=False):
        """Remove and return an element from the front or back of the set.
        Raises KeyError if the set is empty.

        :param desc: bool, False, then return an item from front of set, True then
            return an item from the back of the set
        :returns: tuple (score, elem)
        """

        # redis >=5.0.0
        if desc:
            ret = self.interface.zpopmax(self.key, 1)

        else:
            ret = self.interface.zpopmin(self.key, 1)

#         with self.pipeline() as pipe:
#             pipe.zrange(self.key, 0, 1, desc=desc, withscores=True, score_cast_func=self.normalize_score)
#             if desc:
#                 pipe.zremrangebyrank(self.key, -1, -1)
# 
#             else:
#                 pipe.zremrangebyrank(self.key, 0, 0)
# 
#             ret = pipe.execute()[0]

        if ret:
            ret = ret[0]
            elem = self.normalize_data(self.from_interface(ret[0]))
            ret = (ret[1], elem)

        else:
            raise KeyError()

        return ret

    def rpop(self):
        """convenience method for pop(desc=True), pops from the end of the set instead of the front"""
        return self.pop(desc=True)

    def __len__(self):
        return int(self.interface.zcard(self.key))

    def __contains__(self, elem):
        """Test for membership of *elem* in the set"""
        data = self.to_interface(self.normalize_data(elem))
        rank = self.interface.zrank(self.key, data)
        return rank is not None

    def __iter__(self):
        for score, elem in self.chunk(desc=False):
            yield score, elem

    def __reversed__(self):
        for score, elem in self.chunk(desc=True):
            yield score, elem

    def chunk(self, limit=0, offset=0, chunk=5000, desc=False):
        """return limit elements of the set starting at offset

        this is mainly an internal method for __iter__ and __reversed__

        :param limit: int, how many total items to iterate, 0 means iterate all items
        :param offset: int, what item to start iterating
        :param chunk: int, while iterating to limit pull chunk items at a time
        :param desc: bool, True to go back to front, False to go front to back
        :returns: yields items
        """
        while limit >= 0:
            items = self.interface.zrange(
                self.key,
                offset,
                offset + (chunk - 1),
                desc=desc,
                withscores=True,
                score_cast_func=self.normalize_score,
            )

            count = 0
            for count, item in enumerate(items, 1):
                elem = self.normalize_data(self.from_interface(item[0]))
                yield (item[1], elem)

            offset += count
            #pout.v(count, chunk, limit, offset)
            if count < chunk:
                limit = -1

            else:
                if limit:
                    limit -= count

    def copy(self):
        """Return a local copy divorced from the backend interface

        :returns: list, because we want to maintain order this returns a list
            instead of a set, which isn't optimal since you lose all the set
            functionality on the copy
        """
        return list(self.__iter__())


class SentinelCache(Cache):
    """Creates a cache after the first failed boolean check, handy when you only want
    to do things at certain intervals

    :Example:
        s = SentinelCache("foo")

        if not s:
            # this check passes because there is not sentinel value in the cache

        if not s:
            # this check fails because there is now a sentinel value in the cache
    """
    serialize = False
    default = 0

    def __bool__(self):
        """Return True if there is a cached value at key"""
        ret = False
        if self.exists():
            ret = True

        else:
            # we cache the sentinal after the first failed exists check
            self.increment(1)

        return ret
SentinalCache = SentinelCache # because I can't spell


# class StateCache(BaseCache):
#     """The idea of this is to store the complete state of the object as properties
#     are modified, that way you can quickly keep an object cached without having
#     to selectively decide what to cache or anything"""
# 
#     def __init__(self, key, data=None, **kwargs):
#         #self.__origclass__ = self.__class__
# 
#         for k in ["serialize", "prefix", "ttl", "connection_name", "default"]:
#             kwargs.setdefault(k, getattr(self, k))
# 
#         self._cache = Cache(key, data, **kwargs)
# 
#     def __setattr__(self, name, val):
#         if name in ["_cache"]:
#             #self.__dict__[name] = val
#             super(ObjectCache, self).__setattr__(name, val)
# 
#         else:
#             c = self._cache
#             o = c.data
#             setattr(o, name, val)
#             c.data = o
# 
#     def __delattr__(self, name):
#         c = self._cache
#         o = c.data
#         delattr(o, name)
#         c.data = o
# 
#     def __getattribute__(self, name):
#         if name == "_cache":
#             ret = super(ObjectCache, self).__getattribute__(name)
# 
#         elif name == "__class__":
#             try:
#                 o = self._cache.data
#                 if o is not None:
#                     ret = o.__class__
# 
#             except AttributeError:
#                 ret = super(ObjectCache, self).__getattribute__(name)
# 
#         else:
#             try:
#                 ret = super(ObjectCache, self).__getattribute__(name)
#             except AttributeError:
#                 o = self._cache.data
#                 if o is not None:
#                     ret = getattr(o, name) 
# 
#         return ret


