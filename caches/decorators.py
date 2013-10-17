import types

from . import KeyCache

class FuncDecorator(object):
    """
    A decorator class that you can be extended that allows you to do normal decorators
    with no arguments, or a decorator with arguments

    May be invoked as a simple, argument-less decorator (i.e. `@decorator`) or
    with arguments customizing its behavior (e.g. `@decorator(*args, **kwargs)`).

    based off of the task decorator in Fabric
    https://github.com/fabric/fabric/blob/master/fabric/decorators.py#L15

    other links -- 
    http://pythonconquerstheuniverse.wordpress.com/2012/04/29/python-decorators/
    http://stackoverflow.com/questions/739654/
    http://stackoverflow.com/questions/666216/decorator-classes-in-python
    """
    def __init__(self, *args, **kwargs):

        callables = (
            types.FunctionType,
            types.LambdaType,
            types.MethodType,
            types.UnboundMethodType,
            types.BuiltinFunctionType,
            types.BuiltinMethodType
        )
        if (len(args) == 1) and isinstance(args[0], callables) and not kwargs:
            func, args = args[0], ()
            self.func = func
        else:
            #self.func = func
            self.func = None

        self.handle_args(*args, **kwargs)

    def handle_args(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        invoke = True
        if not self.func:
            func, args = args[0], ()
            self.func = func
            invoke = False

        return self.handle_func(*args, **kwargs) if invoke else self.handle_func

    def handle_func(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class cached(FuncDecorator):
    """
    make caching the return value of a function extremely easy

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

    cache_cls -- class -- The caches class you want to use (defaults to KeyCache)
    key -- string|callback -- if a string, then always use the same key, if a callback
        then the callback should have the same signature as the wrapped function
        beacause the arguments passed to the function are passed to the key func
        to compute the key, the callback should return a list
    **cache_options -- dict -- 
        ttl -- integer -- how long to keep the cache value in the cache
        prefix -- string -- if you want the cache to have a certain prefix on the key
    """
    def handle_args(self, cache_cls=None, key=None, **cache_options):
        self.cache_options = cache_options

        if cache_cls:
            self.cache_cls = cache_cls
        else:
            self.cache_cls = KeyCache

        if key:
            if isinstance(key, types.StringTypes):
                self.key = lambda *args, **kwargs: [key]
            else:
                self.key = key
        else:
            self.key = lambda *args, **kwargs: []
        self.key = key

    def handle_func(self, *args, **kwargs):
        # build the caching object
        key_args = self.key(*args, **kwargs)
        c = self.cache_cls(*key_args)
        for k, n in self.cache_options.iteritems():
            setattr(c, k, n)

        # get/set the cache
        ret = c.data
        if ret is None:
            ret = self.func(*args, **kwargs)
            # cache the result
            if ret is not None:
                c.data = ret

        return ret

