# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import importlib
import os
import json
import types
import itertools
import logging
from contextlib import contextmanager

from caches.compat import *
from caches.core import (
    Cache,
    DictCache,
    SetCache,
    SortedSetCache,
    SentinelCache,
)
from .interface import get_interfaces, get_interface, set_interface
from .dsn import configure, configure_environ
from .decorators import cached


__version__ = '2.0.0'


logger = logging.getLogger(__name__)



def clear_unsafe(pattern):
    """Clear the keys matching pattern

    This uses scan to find keys matching pattern (eg, foo*) and delets them one
    at a time

    :param pattern: str, something like foo* or *bar*
    :returns: int, how many keys were deleted
    """
    count = 0
    for connection_name, inter in get_interfaces().items():
        # https://redis.io/commands/scan
        # https://stackoverflow.com/a/34166690/5006
        for key in inter.scan_iter(match=pattern, count=500):
            inter.delete(key)
            count += 1

    return count


configure_environ()

