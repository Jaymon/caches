from __future__ import absolute_import
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
        beacause the arguments passed to the function are passed to the key func
        to compute the key, the callback should return a list, the list will be passed
        to cache_cls like this: cache_cls(*list_returned_from_key)
    **cache_options -- dict -- 
        ttl -- integer -- how long to keep the cache value in the cache
        prefix -- string -- if you want the cache to have a certain prefix on the key
    """
    def decorate(self, func, cache_cls, key=None, **cache_options):
        if key:
            if isinstance(key, (types.StringTypes, types.IntType, types.LongType, types.FloatType)):
                key = lambda *args, **kwargs: [key]

        else:
            key = lambda *args, **kwargs: []

        def decorator(*args, **kwargs):
            # build the caching object
            key_args = key(*args, **kwargs)
            c = cache_cls(*key_args)
            for k, n in cache_options.iteritems():
                setattr(c, k, n)

            # get/set the cache
            ret = c.data
            if ret is None:
                ret = func(*args, **kwargs)

                # cache the result
                if ret is not None:
                    c.data = ret

            return ret

        return decorator

