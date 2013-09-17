import logging

logger = logging.getLogger(__name__)

class Interface(object):

    connection = None
    """holds the raw connection instance"""

    connected = False
    """True if connection is connected"""

    connection_config = None
    """hold a dict of configuration variables"""

    def __init__(self, **connection_config):
        self.connection_config = connection_config

    def connect(self):
        if self.connected: return self.connected


        self.connection = self._dispatch(self._get_connection, **self.connection_config)
        self.connected = True
        self.log('Connected using config {}', self.connection_config)
        return self.connected

    def _get_connection(self, **connection_config):
        """return a raw connection backend object, so for redis this would be a redis.StrictRedis instance"""
        raise NotImplemented()

    def _dispatch(self, func, *args, **kwargs):
        """So we don't have try catches everywhere..."""
        return func(*args, **kwargs)

    def assure(self):
        self.connect()

    def set(self, key, value, ttl):
        self.assure()
        self.log('SET {} for {}', key, ttl)
        return self._dispatch(self._set, key, value, ttl)

    def _set(self, key, value, ttl):
        raise NotImplemented()

    def multiset(self, keys, values, ttls):
        self.assure()
        self.log('MULTISET {} for {}', keys, ttls)
        return self._dispatch(self._multiset, keys, values, ttls)

    def _multiset(self, keys, values, ttls):
        raise NotImplemented()

    def get(self, key, default_val=None):
        self.assure()
        self.log('GET {}', key)
        ret = self._dispatch(self._get, key)
        if ret is None:
            self.log('GET MISS')
            ret = default_val

        return ret

    def _get(self, key):
        raise NotImplemented()

    def multiget(self, keys, default_val=None):
        self.assure()
        self.log('MULTIGET {}', keys)
        rets = self._dispatch(self._multiget, keys)
        if default_val is not None:
            for i, ret in enumerate(rets):
                if ret is None:
                    self.log('{} MULTIGET MISS', i)
                    rets[i] = default_val

        return rets

    def _multiget(self, keys):
        raise NotImplemented()

    def increment(self, key, delta=1, ttl=0):
        self.assure()
        self.log('INCREMENT {} -> {}', key, delta)
        return self._dispatch(self._increment, key, int(delta), ttl)

    def _increment(self, key, delta, ttl):
        raise NotImplemented()

    def delete(self, key):
        self.assure()
        self.log('DELETE {}', key)
        return self._dispatch(self._delete, key)

    def _delete(self, key):
        raise NotImplemented()

    def multidelete(self, keys):
        self.assure()
        self.log('MULTIDELETE {}', keys)
        return self._dispatch(self._multidelete, keys)

    def _multidelete(self, keys):
        raise NotImplemented()

    def flush(self):
        """this will clear the entire cache db, be careful with this"""
        self.assure()
        self.log('FLUSH DB')
        return self._dispatch(self._flush)

    def _flush(self):
        raise NotImplemented()

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

