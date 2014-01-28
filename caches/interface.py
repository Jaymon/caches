from __future__ import absolute_import
import logging
import itertools

import redis
from redis.client import StrictPipeline, Script

from . import CacheError

logger = logging.getLogger(__name__)

class RedisMixin(object):

    log_key = set(['DEL', 'DUMP', 'EXISTS', 'EXPIRE', 'EXPIREAT', 'MOVE', 'PERSIST',
                'PEXPIRE', 'PEXPIREAT', 'PTTL', 'RENAME', 'RENAMENX', 'RESTORE',
                'SORT', 'TTL', 'TYPE', 'HGETALL', 'HKEYS', 'HLEN', 'HVALS', 'SADD',
                'SCARD', 'SISMEMBER', 'SMEMBERS', 'SPOP', 'SRANDMEMBER', 'SREM', 'ZADD',
                'ZCARD', 'ZCOUNT', 'ZINCRBY', 'ZRANGE', 'ZRANGEBYSCORE', 'ZRANK', 'ZREM',
                'ZREMRANGEBYRANK', 'ZREMRANGEBYSCORE', 'ZREVRANGE', 'ZREVRANGEBYSCORE', 
                'ZREVRANK', 'ZSCORE', 'APPEND', 'BITCOUNT', 'BITOP', 'DECR', 'DECRBY', 
                'GET', 'GETBIT', 'GETRANGE', 'GETSET', 'INCR', 'INCRBY', 'INCRBYFLOAT',
                'MGET', 'MSET', 'MSETNX', 'PSETEX', 'SET', 'SETBIT', 'SETEX', 'SETNX',
                'SETRANGE', 'STRLEN'
              ])

    log_key_field = set(['HDEL', 'HEXISTS', 'HGET', 'HINCRBY', 'HINCRBYFLOAT', 'HSET', 'HSETNX'])

    log_script = set(['EVALSHA'])

    def log(self, format_str, *format_args, **log_options):
        """
        wrapper around the module's logger

        format_str -- string -- the message to log
        *format_args -- list -- if format_str is a string containing {}, then format_str.format(*format_args) is ran
        **log_options --
            level -- something like logging.DEBUG
        """
        log_level = log_options.get('level', logging.DEBUG)
        if logger.isEnabledFor(log_level):
            if format_args:
                logger.log(log_level, format_str.format(*format_args))
            else:
                logger.log(log_level, format_str)

    def pipeline(self, transaction=True, shard_hint=None):
        pipeline = RedisPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint
        )

        return pipeline


class RedisPipeline(RedisMixin, StrictPipeline):
    def _execute_pipeline(self, connection, commands, raise_on_error):
        self.log('Execute {} Pipeline commands', len(commands))
        res = super(RedisPipeline, self)._execute_pipeline(connection, commands, raise_on_error)
        return res

    def _execute_transaction(self, connection, commands, raise_on_error):
        self.log('Execute {} Transaction commands', len(commands))
        res = super(RedisPipeline, self)._execute_transaction(connection, commands, raise_on_error)
        return res

    def execute_command(self, *args, **kwargs):
        if args[0] in self.log_key:
            self.log("{} QUEUED - {}", args[0], args[1])
        elif args[0] in self.log_key_field:
            self.log("{} QUEUED - {} > {}", args[0], args[1], args[2])
        elif args[0] in self.log_script:
            self.log("LUA QUEUED - {}", args[3])

        return super(RedisPipeline, self).execute_command(*args, **kwargs)


class Redis(RedisMixin, redis.StrictRedis):
    def __init__(self, **connection_config):
        connection_config['socket_timeout'] = float(connection_config.get('socket_timeout', 1.0))
        connection_config['port'] = int(connection_config.get('port') or 6379)
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

    def flush(self):
        """this will clear the entire cache db, be careful with this"""
        self.log('FLUSH DB {}', self.connection_pool.connection_kwargs['db'])
        #return self._dispatch(self._flush)
        return self.flushdb()

    def execute_command(self, *args, **kwargs):
        if args[0] in self.log_key:
            self.log("{} - {}", args[0], args[1])
        elif args[0] in self.log_key_field:
            self.log("{} - {} > {}", args[0], args[1], args[2])
        elif args[0] in self.log_script:
            self.log("LUA - {}", args[3])

        res = super(Redis, self).execute_command(*args, **kwargs)
        return res

