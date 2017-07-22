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
from .interface import get_interfaces, get_interface, set_interface


__version__ = '0.2.18'

logger = logging.getLogger(__name__)


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
    c = dsnparse.parse(dsn)
    if c.fragment in get_interfaces():
        raise ValueError('a connection named "{}" has already been configured'.format(c.fragment))

    # compensate for passing pw as username (eg, not doing //:password@ but instead //password@)
    password = None
    if c.password:
        password = c.password
    elif c.username:
        password = c.username

    interface_module_name, interface_class_name = c.scheme.rsplit('.', 1)
    interface_module = importlib.import_module(interface_module_name)
    interface_class = getattr(interface_module, interface_class_name)

    connection_config = dict(host=c.host, port=c.port, password=password, **c.query)
    paths = c.paths
    if paths:
        connection_config['db'] = paths[0]

    i = interface_class(**connection_config)
    set_interface(i, c.fragment)
    return i



# class SortedSetCache(Cache, SortedSet):
#     """
#     A sorted set cache that uses Redis' zset behing the scene and gives a very
#     similar api similar to Python's built-in set
#     """
#     def add(self, elem, score=1):
#         ret = False
#         if self.ttl:
#             with self.redis.pipeline() as pipe:
#                 self._add(elem, pipe=pipe, score=score)
#                 pipe.expire(self.key, self.ttl)
#                 ret = pipe.execute()[0]
# 
#         else:
#             ret = super(SortedSetCache, self).add(elem, score)
# 
#         return bool(ret)
# 
#     def addnx(self, elem, score=1):
#         ret = False
#         if self.ttl:
#             with self.redis.pipeline() as pipe:
#                 self._addnx(elem, pipe=pipe, score=score)
#                 pipe.expire(self.key, self.ttl)
#                 ret = pipe.execute()[0]
# 
#         else:
#             ret = super(SortedSetCache, self).addnx(elem, score)
# 
#         return bool(ret)
# 
# 
# class DictCache(Cache, Dict):
#     """
#     A Python dict but in Redis
#     """
#     def __setitem__(self, key, value):
#         """Set ``d[key]`` to *value*."""
#         value = self._pickle(value)
#         if self.ttl:
#             with self.redis.pipeline() as pipe:
#                 pipe.hset(self.key, key, value)
#                 pipe.expire(self.key, self.ttl)
#                 pipe.execute()
# 
#         else:
#             p = self.redis.hset(self.key, key, value)
# 
# 
# class SetCache(Cache, Set):
#     """
#     A Python set but in Redis
#     """
#     def add(self, elem):
#         ret = False
#         elem = self._pickle(elem)
#         if self.ttl:
#             with self.redis.pipeline() as pipe:
#                 pipe.sadd(self.key, elem)
#                 pipe.expire(self.key, self.ttl)
#                 ret = bool(pipe.execute()[0])
# 
#         else:
#             ret = bool(self.redis.sadd(self.key, elem))
# 
#         return ret
# 
# 
# class CounterCache(Cache, Counter):
#     """
#     A Python collections.Counter instance, but in Redis
# 
#     http://docs.python.org/2/library/collections.html#collections.Counter
#     """
#     def __setitem__(self, key, value):
#         """Set ``d[key]`` to *value*."""
#         value = self._pickle(value)
#         if self.ttl:
#             with self.redis.pipeline() as pipe:
#                 pipe.hset(self.key, key, value)
#                 pipe.expire(self.key, self.ttl)
#                 pipe.execute()
# 
#         else:
#             self.redis.hset(self.key, key, value)
# 
# 
configure_environ()

