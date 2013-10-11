# stdlib
import importlib
import os
import json
import types
import itertools
import logging

from redis_collections import Dict, Set, RedisCollection

try:
    import cPickle as pickle
except ImportError:
    import pickle as pickle

# 3rd party
import dsnparse

__version__ = '0.2'

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

    interface_module_name, interface_class_name = c.scheme.rsplit('.', 1)
    interface_module = importlib.import_module(interface_module_name)
    interface_class = getattr(interface_module, interface_class_name)

    i = interface_class(host=c.host, port=c.port, db=c.paths[0], **c.query)
    set_interface(i, c.fragment)
    return i

def set_interface(interface, name=''):
    """
    don't want to bother with a dsn? Use this method to make an interface available
    """
    assert interface, 'interface is empty'

    global interfaces
    logger.debug('connection_name: {} -> {}'.format(name, interface.__class__.__name__))
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
    serialize = True
    """true to make it pickle values"""

    prefix = ''
    """set the key prefix"""

    ttl = 7200
    """how long to cache the result, 0 for unlimited"""

    connection_name = ''
    """the interface you want to use"""

    interface = None
    """the interface to use for this cache class"""

    key_args = None

    def __init__(self, *args, **kwargs):
        self.key_args = args
        #self.val = kwargs.get('val', None)
        #self.redis = get_interface(self.connection_name)
        super(Cache, self).__init__(data=kwargs.get('data', None))

    @classmethod
    def create(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def has(self):
        """return True if the key exists in Redis"""
        return bool(self.redis.exists(self.key))

    def _create_key(self):
        return u'.'.join(map(unicode, [self.prefix] + list(self.key_args)))

    def _pickle(self, val):
        if not self.serialize: return val
        return pickle.dumps(val, pickle.HIGHEST_PROTOCOL)

    def _unpickle(self, val):
        if val is None: return None
        if not isinstance(val, types.StringType):
            raise TypeError('Only strings can be unpickled (%r given).' % val)
        if not self.serialize: return val
        return pickle.loads(val)

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


class DictCache(Cache, Dict):
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


class SetCache(Cache, Set):
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
            res = redis.set(key, data)


configure_environ()

