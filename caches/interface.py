from __future__ import absolute_import
import logging
import itertools

import redis

from . import CacheError

logger = logging.getLogger(__name__)

class Redis(redis.StrictRedis):
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

#    def execute_command(self, *args, **kwargs):
#        self.log(args[0])
#        return super(Redis, self).execute_command(*args, **kwargs)

    def assure(self):
        self.connect()

    def flush(self):
        """this will clear the entire cache db, be careful with this"""
        self.log('FLUSH DB')
        #return self._dispatch(self._flush)
        return self.flushdb()

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

