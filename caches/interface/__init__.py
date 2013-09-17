
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
        return self._dispatch(self._set, key, value, ttl)

    def _set(self, key, value, ttl):
        raise NotImplemented()

    def multiset(self, keys, values, ttls):
        self.assure()
        return self._dispatch(self._multiset, keys, values, ttls)

    def _multiset(self, keys, values, ttls):
        raise NotImplemented()

    def get(self, key, default_val=None):
        self.assure()
        ret = self._dispatch(self._get, key)
        if ret is None:
            ret = default_val

        return ret

    def _get(self, key):
        raise NotImplemented()

    def multiget(self, keys, default_val=None):
        self.assure()
        rets = self._dispatch(self._multiget, keys)
        if default_val is not None:
            for i, ret in enumerate(rets):
                if ret is None:
                    rets[i] = default_val

        return rets

    def _multiget(self, keys):
        raise NotImplemented()

    def increment(self, key, delta=1, ttl=0):
        self.assure()
        return self._dispatch(self._increment, key, int(delta), ttl)

    def _increment(self, key, delta, ttl):
        raise NotImplemented()

    def delete(self, key):
        self.assure()
        return self._dispatch(self._delete, key)

    def _delete(self, key):
        raise NotImplemented()

    def multidelete(self, keys):
        self.assure()
        return self._dispatch(self._multidelete, keys)

    def _multidelete(self, keys):
        raise NotImplemented()

    def flush(self):
        """this will clear the entire cache db, be careful with this"""
        self.assure()
        return self._dispatch(self._flush)

    def _flush(self):
        raise NotImplemented()

