from __future__ import absolute_import
import itertools

import redis

from . import Interface
from .. import CacheError

class RedisInterface(Interface):

    def _dispatch(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.RedisError as e:
            raise CacheError(e)

    def _get_connection(self, host, port, db, **options):
        redis_kwargs = dict(options)
        redis_kwargs.setdefault('socket_timeout', float(options.get('socket_timeout', 1.0)))
        redis_kwargs.update({
            'host': host,
            'db': db,
            'port': int(port or 6379)
        })
        return redis.StrictRedis(**redis_kwargs)

    def _set(self, key, value, ttl):
        if ttl:
            res = self.connection.setex(key, ttl, value)

        else:
            res = self.connection.set(key, value)

        return res

    def _multiset(self, keys, values, ttls):
        r = self.connection.pipeline()
        for key, ttl, val in itertools.izip(keys, ttls, values):
            if ttl:
                r.setex(key, ttl, val)
            else:
                r.set(key, val)

        self._dispatch(r.execute)
        return True

    def _get(self, key):
        return self.connection.get(key)

    def _multiget(self, keys):
        return self.connection.mget(keys)

    def _increment(self, key, delta, ttl):
        if ttl:
            r = self.connection.pipeline()
            res = r.incr(key, delta)
            r.expire(key, ttl)
            res = r.execute()[0]

        else:
            res = self.connection.incr(key, delta)

        return res

    def _delete(self, key):
        return self.connection.delete(key)

    def _multidelete(self, keys):
        return self.connection.delete(*keys)

    def _flush(self):
        return self.connection.flushdb()


