# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import importlib

import dsnparse

from .compat import *
from .interface import set_interface


class DSN(dsnparse.ParseResult):
    @property
    def connection_name(self):
        return self.fragment

    @property
    def interface_class(self):
        interface_module_name, interface_class_name = self.scheme.rsplit('.', 1)
        interface_module = importlib.import_module(interface_module_name)
        interface_class = getattr(interface_module, interface_class_name)
        return interface_class

    def interface(self):
        return self.interface_class(**self.connection_config())

    def connection_config(self):
        connection_config = dict(
            host=self.host,
            port=self.port,
            password=self.password,
            **self.query
        )
        paths = self.paths
        if paths:
            connection_config['db'] = paths[0]
        return connection_config

    def configure(self):
        self.scheme = self.configure_scheme(self.scheme)
        self.password = self.configure_password(self.username, self.password)

        if not self.port:
            self.port = 6379

        self.query["socket_timeout"] = float(self.query.get("socket_timeout", 1.0))

    def configure_scheme(self, v):
        ret = v
        d = {
            "caches.interface.Redis": set(["redis"]),
        }

        kv = v.lower()
        for interface_name, vals in d.items():
            if kv in vals:
                ret = interface_name
                break

        return ret

    def configure_password(self, username, password):
        # compensate for passing pw as username (eg, not doing //:password@ but instead //password@)
        ret = None
        if password:
            ret = password
        elif username:
            ret = username
        return ret


def configure_environ(dsn_env_name='CACHES_DSN', parse_class=DSN):
    """configure interfaces based on environment variables

    by default, when caches is imported, it will look for CACHES_DSN, and CACHES_DSN_N (where
    N is 1 to infinity) in the environment, if it finds them, it will assume they
    are dsn urls and will configure connections with them. If you don't want this
    behavior (ie, you want to configure caches manually) then just make sure
    you don't have any environment variables with matching names The num checks
    (eg CACHES_DSN_1, CACHES_DSN_2) go in order, so you can't do CACHES_DSN_1, CACHES_DSN_3,
    because it will fail on _2 and move on, so make sure your N dsns are in order
    (eg, 1, 2, 3, ...)

    :example:
        export CACHES_DSN_1=redis://host:port/dbname#conn_name_1
        export CACHES_DSN_2=redis://host2:port/dbname2#conn_name_2
        $ python
        >>> import caches.interface
        >>> print caches.interface.interfaces # prints a dict with interfaces conn_name_1 and
        conn_name_2 keys

    :param dsn_env_name: string, the name of the environment variables
    :param parse_class: dsnparse.ParseResult, the class that will hold the result
    :returns: list, a list of the found interfaces
    """
    inters = []
    cs = dsnparse.parse_environs(dsn_env_name, parse_class=parse_class)
    for c in cs:
        inter = c.interface()
        set_interface(inter, c.connection_name)
        inters.append(inter)
    return inters


def configure(dsn, parse_class=DSN):
    """
    configure an interface to be used to query a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    example DSNs:

       redis://localhost:6379/0?timeout=5
       module.path.Classname://localhost:6379/0?timeout=5

    :param dsn: string, a properly formatted prom dsn, see DsnConnection for how to format the dsn
    :param parse_class: dsnparse.ParseResult, the class that will hold the result
    :returns: Interface, the found interface class
    """
    c = dsnparse.parse(dsn, parse_class=parse_class)
    inter = c.interface()
    set_interface(inter, c.connection_name)
    return inter

