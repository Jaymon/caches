# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import types

from decorators import FuncDecorator


class cached(FuncDecorator):
    """
    make caching the return value of a function extremely easy

    This only really works for KeyCache derived caching

    based off of https://github.com/jayferd/python-cache

    example -- 
        @cached(key="some_cache_key")
        def foo(*args):
            return reduce(lambda x, y: x+y, args)

        foo(1, 2) # will compute the value and cache the return value
        foo(1, 2) # return value from cache

        foo(1, 2, 3) # uh-oh, wrong value, our key was too static

        # let's try again, this time with a dynamic key
        @cached(key=lambda *args: args)
        def foo(*args):
            return reduce(lambda x, y: x+y, args)

        foo(1, 2) # compute and cache, key func returned [1, 2]
        foo(1, 2) # grabbed from cache
        foo(1, 2, 3) # compute and cache because our key func returned [1, 2, 3]

    cache_cls -- class -- The caches class you want to use
    key -- string|callback -- if a string, then always use the same key, if a callback
        then the callback should have the same signature as the wrapped function
        because the arguments passed to the function are passed to the key func
        to compute the key, the callback should return a list, the list will be passed
        to cache_cls like this: cache_cls(*list_returned_from_key)
    **cache_options -- dict -- 
        ttl -- integer -- how long to keep the cache value in the cache
        prefix -- string -- if you want the cache to have a certain prefix on the key
        canary -- mixed -- the sentinal value to check for, defaults to None
    """
    def decorate(self, func, cache_cls, key=None, **cache_options):
        canary = cache_options.pop('canary', None)
        if key:
            if callable(key):
                key_cb = key
            else:
                key_cb = lambda *args, **kwargs: [key]

        else:
            key_cb = lambda *args, **kwargs: []

        def decorator(*args, **kwargs):
            # build the caching object
            key_args = key_cb(*args, **kwargs)
            if isinstance(key_args, basestring):
                key_args = [key_args]

            c = cache_cls(*key_args)
            for k, n in cache_options.iteritems():
                setattr(c, k, n)

            # get/set the cache
            ret = c.data
            if ret is canary:
                ret = func(*args, **kwargs)

                # cache the result
                if ret is not canary:
                    c.data = ret

            return ret

        return decorator


class classproperty(property):
    """
    allow a class property to exist on the Orm
    NOTE -- this is read only, you can't write to the property
    example --
        class Foo(object):
            @classproperty
            def bar(cls):
                return 42
        Foo.bar # 42
    http://stackoverflow.com/questions/128573/using-property-on-classmethods
    http://stackoverflow.com/questions/5189699/how-can-i-make-a-class-property-in-python
    http://docs.python.org/2/reference/datamodel.html#object.__setattr__
    """
    def __get__(self, instance, cls):
        return self.fget(cls)


