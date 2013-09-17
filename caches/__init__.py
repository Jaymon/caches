# stdlib
import importlib
import os
import json
import types
import itertools
import logging

# 3rd party
import dsnparse

__version__ = '0.1.1'

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

    json = True
    """true to make it json"""

    prefix = ''
    """set the key prefix"""

    ttl = 7200
    """how long to cache the result, 0 for unlimited"""

    version = 1
    """the key version"""

    # TODO -- implement this
    raise_errors = False
    """true if this class wants to raise errors instead of suprressing them"""

    connection_name = ''
    """the interface you want to use"""

    interface = None
    """the interface to use for this cache class"""

    @property
    def key(self):
        return self.keys[0] if len(self.keys) else None

    @property
    def val(self):
        return self.vals[0] if len(self.vals) else None

    @val.setter
    def val(self, v):
        self.vals[0] = v

    def __init__(self, *args, **kwargs):
        self.keys = []
        self.vals = []
        if 'val' in kwargs:
            self.add_key(*args, **kwargs)
        else:
            self.add_keys(*args, **kwargs)

        self.interface = get_interface(self.connection_name)

    @classmethod
    def create(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def is_multi(self):
        return len(self.keys) > 1

    def add_key(self, *args, **kwargs):
        self.keys.append(u'|'.join(map(unicode, [self.prefix, self.version] + list(args))))
        self.vals.append(kwargs.get('val', None))

    def add_keys(self, *args, **kwargs):
        list_size = -1
        arg_info = []
        for i, x in enumerate(args):
            arg_info.append([False, x])
            if not isinstance(x, types.StringTypes) and hasattr(x, '__iter__'):
                arg_info[i][0] = True
                arg_info[i][1] = list(x)
                arg_list_size = len(arg_info[i][1])
                if list_size >= 0:
                    assert arg_list_size == list_size, "cannot have 2 list key args of different sizes"

                else:
                    list_size = arg_list_size

            else:
                arg_info[i][1] = x

        list_size = 1 if list_size < 0 else list_size
        def make_list(arg_info, list_size):
            if arg_info[0]:
                return arg_info[1]
            else:
                return itertools.repeat(arg_info[1], list_size)

        vals = kwargs.get('vals', [])
        for i, arg in enumerate(itertools.izip_longest(*(make_list(x, list_size) for x in arg_info))):
            self.add_key(*arg, val=vals[i] if len(vals) > i else None)

    def get(self, default_val=None):
        vals = []
        is_multi = self.is_multi()
        if is_multi:
            vals = self.interface.multiget(self.keys, default_val)
        else:
            vals = [self.interface.get(self.key, default_val)]

        if self.json:
            for i, val in enumerate(vals):
                if val != default_val:
                    vals[i] = json.loads(val)

        self.vals = vals
        return self.vals if is_multi else self.vals[0]

    def increment(self, val=None):
        assert len(self.keys) == 1, "increment does not work with multi cache queries"
        if val is None:
            val = self.val
            if val is None:
                val = 1

        return self.interface.increment(self.key, val, self.ttl)

    def set(self):
        is_multi = self.is_multi()

        vals = ((json.dumps(val) if self.json else val) for val in self.vals)
        if is_multi:
            self.interface.multiset(self.keys, vals, itertools.repeat(self.ttl, len(self.keys)))
        else:
            val = list(vals)[0]
            self.interface.set(self.key, val, self.ttl)

    def delete(self):
        is_multi = self.is_multi()
        if is_multi:
            self.interface.multidelete(self.keys)
        else:
            self.interface.delete(self.key)

configure_environ()
