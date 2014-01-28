"""
Just some other collections on top of what redis_collections gives us

would a dequeue be handy?
http://docs.python.org/2/library/collections.html#collections.deque

http://docs.python.org/2/library/collections.html#collections-abstract-base-classes
"""
from __future__ import absolute_import
import collections
from itertools import imap

from redis_collections import RedisCollection


class SortedSet(RedisCollection, collections.MutableSet):
    """
    Sorted set uses a similar api to Python's built in set but offers the ability
    to add a rank to each element in order to sort the set

    incompatibilities with Python set --
        add() -- takes a score value
        pop() -- retuns a tuple (elem, score) while a normal set would just return elem
        __iter__ -- returns tuples like pop()
        addnx -- addition to native api, will only add the elem if it doesn't already exist

        currently there is no union or intersect support
    
    http://docs.python.org/2/library/stdtypes.html#set
    http://code.activestate.com/recipes/576694/
    http://stackoverflow.com/questions/1653970/does-python-have-an-ordered-set
    http://stackoverflow.com/questions/5953205/sorted-container-in-python
    """
    def __init__(self, *args, **kwargs):
        super(SortedSet, self).__init__(*args, **kwargs)

        # https://github.com/antirez/redis/issues/1128
        # https://github.com/antirez/redis/pull/1132
        lua = """
        local ret_value = 0
        if not redis.call('ZSCORE', KEYS[1], ARGV[2]) then
          redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
          ret_value = 1
        end
        return ret_value"""
        self.lua_addnx = self.redis.register_script(lua)
        #pout.v(self.lua_addnx)

    def __len__(self):
        return int(self.redis.zcard(self.key))

    def __contains__(self, elem):
        """Test for membership of *elem* in the queue"""
        rank = self.redis.zrank(self.key, self._pickle(elem))
        return False if rank is None else True

    def remove(self, elem):
        removed_count = self.redis.zrem(self.key, self._pickle(elem))
        if not removed_count:
            raise KeyError('elem was not in the set')

    def discard(self, elem):
        return self.redis.zrem(self.key, self._pickle(elem))

    def _data(self, pipe=None, last=False):
        redis = pipe if pipe is not None else self.redis
        limit = 500
        offset = 0
        while True:
            vals = redis.zrange(
                self.key,
                offset,
                offset + limit,
                desc=last,
                withscores=True,
                score_cast_func=int
            )

            if vals:
                for v in vals:
                    yield self._unpickle(v[0]), v[1]

                offset += limit
                offset += 1

            else:
                break

    def __iter__(self):
        return self._data(last=False)

    def __reversed__(self):
        return self._data(last=True)

    def _update(self, data, pipe=None):
        super(SortedSet, self)._update(data, pipe)
        p = pipe if pipe is not None else self.redis.pipeline()

        for elem in data:
            self._add(elem, score=1, pipe=p)

        #pout.v(p, exe)
        if pipe is None:
            p.execute()

    def add(self, elem, score=1):
        return bool(self._add(elem, score=score, pipe=self.redis))

    def addnx(self, elem, score=1):
        return bool(self._addnx(elem, score=score, pipe=self.redis))

    def _add(self, elem, score, pipe):
        return pipe.zadd(self.key, score, self._pickle(elem))

    def _addnx(self, elem, score, pipe):
        return self.lua_addnx(keys=[self.key], args=[score, self._pickle(elem)], client=pipe)


    def pop(self, last=False):
        ret = None
        with self.redis.pipeline() as pipe:
            pipe.zrange(self.key, 0, 1, desc=last, withscores=True, score_cast_func=int)
            if last:
                pipe.zremrangebyrank(self.key, -1, -1)

            else:
                pipe.zremrangebyrank(self.key, 0, 0)

            ret = pipe.execute()[0]
            if ret: ret = ret[0]

        return (self._unpickle(ret[0]), ret[1]) if ret else (None, 0)

    def rpop(self):
        """just like pop, but pops from the end of the set instead of the front"""
        return self.pop(last=True)

    def union(*args, **kwargs):
        raise NotImplemented()

    def intersection(*args, **kwargs):
        raise NotImplemented()

    def difference(*args, **kwargs):
        raise NotImplemented()

    def symmetric_difference(*args, **kwargs):
        raise NotImplemented()

    def issubset(*args, **kwargs):
        raise NotImplemented()

    def issuperset(*args, **kwargs):
        raise NotImplemented()


class CountingSet(SortedSet):
    """
    Implements a counting set (which counts duplicates of the same elem in the set)
    using a Redis sorted set, unlike Redis, this will return items from highest to
    lowest by default, and by lowest to highest using reversed(self)
    """
    def _add(self, elem, pipe, score=1):
        return pipe.zincrby(self.key, self._pickle(elem), score)

    def __iter__(self):
        return self._data(last=True)

    def __reversed__(self):
        return self._data(last=False)

    def pop(self, last=True):
        return super(CountingSet, self).pop(last)

class PriorityQueue(RedisCollection):
    # TODO -- I think this can be a full blown implementation of a Queue and PriorityQueue
    # but I don't have time to do it right now since I'm going to instead do an ordered set.
    # Might want to look at Redis's pubsub stuff to implement this also instead of sortedset
    # if you do use this instead of pub/sub, zremrangebyrank will allow you to limit
    # the set size, likewise a List might be handy here with its LPOP and LPUSH methods
    # http://docs.python.org/2/library/queue.html#module-Queue
    # http://oldblog.antirez.com/post/250

    def __len__(self):
        return self.qsize()

    def qsize(self):
        """Return the approximate size of the queue"""
        return self.redis.zcard(self.key)

    def empty(self):
        return True if len(self) > 0 else False

    def _data(self, pipe=None):
        redis = pipe if pipe is not None else self.redis
        return (self._unpickle(v) for v in redis.zrange(self.key, 0, -1))

    def __iter__(self):
        return self._data()

    def __contains__(self, elem):
        """Test for membership of *elem* in the queue"""
        rank = self.redis.zrank(self.key, self._pickle(elem))
        return False if rank is None else True

    def put(self, elem, rank=0):
        return bool(self.redis.zadd(self.key, rank, self._pickle(elem)))

    def get(self):
        return self._unpickle(redis.zrange(self.key, 0, 1))


    def _update(self, data, pipe=None):
        super(PriorityQueue, self)._update(data, pipe)
        redis = pipe if pipe is not None else self.redis

        others = [data] + list(others or [])
        elements = map(self._pickle, data)

        redis.zadd(self.key, *elements)

