# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import logging

import redis
from redis.client import Pipeline
from redis.commands.core import Script
from datatypes import LogMixin

from .compat import *
from .exception import CacheError


logger = logging.getLogger(__name__)


interfaces = {}
"""holds all the configured interfaces"""


def get_interfaces():
    global interfaces
    return interfaces


def get_interface(name=''):
    """
    get an interface that was created using configure()

    name -- string -- the name of the connection for the interface to return
    """
    global interfaces
    return interfaces[name]


def set_interface(interface, name=''):
    """
    don't want to bother with a dsn? Use this method to make an interface available
    """
    if not interface:
        raise ValueError('interface is empty')

    global interfaces
    logger.debug('connection_name: "{}" -> {}.{}'.format(
        name,
        interface.__module__,
        interface.__class__.__name__
    ))
    interfaces[name] = interface


class RedisMixin(LogMixin):
    log_key = set([
        'DEL', 'DUMP', 'EXISTS', 'EXPIRE', 'EXPIREAT', 'MOVE', 'PERSIST',
        'PEXPIRE', 'PEXPIREAT', 'PTTL', 'RENAME', 'RENAMENX', 'RESTORE',
        'SORT', 'TTL', 'TYPE', 'HGETALL', 'HKEYS', 'HLEN', 'HVALS', 'SADD',
        'SCARD', 'SISMEMBER', 'SMEMBERS', 'SPOP', 'SRANDMEMBER', 'SREM',
        'ZADD', 'ZCARD', 'ZCOUNT', 'ZINCRBY', 'ZRANGE', 'ZRANGEBYSCORE',
        'ZRANK', 'ZREM', 'ZREMRANGEBYRANK', 'ZREMRANGEBYSCORE', 'ZREVRANGE',
        'ZREVRANGEBYSCORE', 'ZREVRANK', 'ZSCORE', 'APPEND', 'BITCOUNT', 'BITOP',
        'DECR', 'DECRBY', 'GET', 'GETBIT', 'GETRANGE', 'GETSET', 'INCR',
        'INCRBY', 'INCRBYFLOAT', 'MGET', 'MSET', 'MSETNX', 'PSETEX', 'SET',
        'SETBIT', 'SETEX', 'SETNX', 'SETRANGE', 'STRLEN',
    ])

    log_key_field = set(['HDEL',
        'HEXISTS',
        'HGET',
        'HINCRBY',
        'HINCRBYFLOAT',
        'HSET',
        'HSETNX'
    ])

    log_script = set(['EVALSHA'])

    def log_call(self, args, res, is_pipe=False, **log_options):
        log_level = log_options.get('level', logging.DEBUG)
        if not self.is_logging(log_level): return
        #if not logger.isEnabledFor(log_level): return

        format_log = '{} QUEUED' if is_pipe else '{}'
        format_args = []

        if args[0] in self.log_key:
            format_log += ' - {}'
            format_args.append(args[0])
            format_args.append(args[1])

            #if len(args) > 2:
            #    format_log += " - VALUES: {}"
            #    format_args.append(args[2:])

        elif args[0] in self.log_key_field:
            format_log += ' - {} > {}'
            format_args.append(args[0])
            format_args.append(args[1])
            format_args.append(args[2])

            #if len(args) > 3:
            #    format_log += " - VALUES: {}"
            #    format_args.append(args[3:])

        elif args[0] in self.log_script:
            format_log += ' - {}'
            format_args.append('LUA')
            format_args.append(args[3])

        else:
            format_args.append(args[0])

        if not is_pipe:
            print_res = res is None or isinstance(res, bool) or \
                    isinstance(res, (float, int, long))

            if print_res:
                format_log += ' ... {}'
                format_args.append(res)

            else:
                if isinstance(res, basestring):
                    format_log += ' ... string'

                elif isinstance(res, list):
                    format_log += ' ... list {}'
                    format_args.append(len(res))

                elif isinstance(res, dict):
                    format_log += ' ... dict {}'
                    format_args.append(len(res))

                else:
                    format_log += ' ... unknown'

        self.log(format_log, *format_args)

    def pipeline(self, transaction=True, shard_hint=None):
        pipeline = RedisPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint
        )

        return pipeline


class RedisPipeline(RedisMixin, Pipeline):
    def _execute_pipeline(self, connection, commands, raise_on_error):
        self.log('Execute {} Pipeline commands', len(commands))
        res = super(RedisPipeline, self)._execute_pipeline(
            connection,
            commands,
            raise_on_error
        )
        return res

    def _execute_transaction(self, connection, commands, raise_on_error):
        self.log('Execute {} Transaction commands', len(commands))
        res = super(RedisPipeline, self)._execute_transaction(
            connection,
            commands,
            raise_on_error
        )
        return res

    def execute_command(self, *args, **kwargs):
        res = super(RedisPipeline, self).execute_command(*args, **kwargs)
        self.log_call(args, res, is_pipe=True)
        return res


class Redis(RedisMixin, redis.StrictRedis):
    """
    https://github.com/andymccurdy/redis-py
    https://github.com/andymccurdy/redis-py/blob/master/redis/commands.py
    """
    def __init__(self, **connection_config):
        try:
            super(Redis, self).__init__(**connection_config)
            self.log('Connected using config {}', connection_config)

        except redis.RedisError as e:
            raise CacheError(e)

    def __getattribute__(self, name):
        """
        http://stackoverflow.com/questions/6602256/python-wrap-all-functions-in-a-library
        """
        try:
            ret = super(Redis, self).__getattribute__(name)

        except redis.RedisError as e:
            raise CacheError(e)

        return ret

    def unsafe_flush(self):
        """this will clear the entire cache db, be careful with this"""
        self.log('FLUSH DB {}', self.connection_pool.connection_kwargs['db'])
        return self.flushdb()

    def execute_command(self, *args, **kwargs):
        res = super(Redis, self).execute_command(*args, **kwargs)
        self.log_call(args, res)
        return res

