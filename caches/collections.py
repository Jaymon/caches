from __future__ import absolute_import
import collections
from itertools import imap

from redis_collections import RedisCollection


class CountingSet(RedisCollection, collections.MutableSet):
    """
    Implements a counting set (which counts duplicates of the same elem in the set)
    using a Redis sorted set, unlike Redis, this will return items from highest to
    lowest by default, and by lowest to highest using reversed(self)
    """
    def __init__(self, *args, **kwargs):
        super(CountingSet, self).__init__(*args, **kwargs)

    def __len__(self):
        return int(self.redis.zcard(self.key))

    def __contains__(self, elem):
        """Test for membership of *elem* in the queue"""
        rank = self.redis.zrank(self.key, self._pickle(elem))
        return False if rank is None else True

    def add(self, elem, rank=1):
        return self.redis.zincrby(self.key, self._pickle(elem), rank)

    def remove(self, elem):
        removed_count = self.redis.zrem(self.key, self._pickle(elem))
        if not removed_count:
            raise KeyError('elem was not in the set')

    def discard(self, elem):
        return self.redis.zrem(self.key, self._pickle(elem))

    def pop(self, last=True):
        ret = None
        with self.redis.pipeline() as pipe:
            if last:
                pipe.zrange(self.key, 0, 1, desc=last, withscores=True, score_cast_func=int)
                pipe.zremrangebyrank(self.key, -1, -1)

            else:
                pipe.zrange(self.key, 0, 1, desc=False, withscores=True, score_cast_func=int)
                pipe.zremrangebyrank(self.key, 0, 0)

            ret = pipe.execute()[0]
            if ret: ret = ret[0]

        return (self._unpickle(ret[0]), ret[1]) if ret else (None, 0)

    def _data(self, pipe=None, last=True):
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
        return self._data()

    def __reversed__(self):
        return self._data(last=False)

    def _update(self, data, pipe=None):
        super(CountingSet, self)._update(data, pipe)
        p = pipe if pipe is not None else self.redis.pipeline()

        for elem in data:
            p.zincrby(self.key, self._pickle(elem), 1)

        #pout.v(p, exe)
        if pipe is None:
            p.execute()


class PriorityQueue(RedisCollection):
    # TODO -- I think this can be a full blown implementation of a Queue and PriorityQueue
    # but I don't have time to do it right now since I'm going to instead do an ordered set.
    # Might want to look at Redis's pubsub stuff to implement this also instead of sortedset
    # if you do use this instead of pub/sub, zremrangebyrank will allow you to limit
    # the set size

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

