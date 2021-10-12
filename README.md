# Caches

A Python caching library that gives a similar interface to standard Python data structures like Dict and Set but is backed by [Redis](https://redis.io).

Caches has been used in production for years across many different products, handling millions of requests.


## How to use

Caches can only use [Redis](http://redis.io).

Caches relies on setting the environment variable `CACHES_DSN`:

    caches.interface.Redis://localhost/0

If you want to cache things using more than one redis server, you can actually set multiple environment variables:

    export CACHES_DSN_1=caches.interface.Redis://somedomain.com/0#redis1
    export CACHES_DSN_2=caches.interface.Redis://someotherdomain.com/0#redis2

After you've set the environment variable, then you just need to import caches in your code:

```python
import caches
```

Caches will take care of parsing the url and creating the redis connection, automatically, so after the import Caches will be ready to use.


### Interface

All caches caching classes have a similar interface, their constructor takes a key and data:

```python
c = Cache(['foo', 'bar', 'che'])
print c.key # foo.bar.che
```

If you would like to init your cache object with a value, use the `data` `**kwarg`:

```python
c = Cache('foo', data="boom!")
print c.key # foo
print c # "boom!"
```

Each caches base caching class is meant to be extended so you can set some parameters:

* **serialize** -- boolean -- True if you want all values pickled, False if you don't (ie, you're caching ints or strings or something).

* **prefix** -- string -- This will be prepended to the key args you pass into the constructor.

* **ttl** -- integer -- time to live, how many seconds to cache the value. 0 (default) means cache forever.

* **connection_name** -- string -- if you have more than one caches dsn then you can use this to set the name of the connection you want (the name of the connection is the `#connection_name` fragment of a dsn url).

```python
class MyIntCache(Cache):
  serialize = False # don't bother to serialize values since we're storing ints
  prefix = "MyIntCache" # every key will have this prefix, change to invalidate all currently cached values
  ttl = 7200 # store each int for 2 hours
```

### Cache Classes


#### Cache

This is the traditional caching object, it sets a value into a key:

```python
c = Cache('foo')
c.data = 5 # cache 5
c += 10 # increment 5 by 10, store 15 in the cache

c.clear()
print c # None
```


#### DictCache

This caching object acts more or less like a Python [dictionary](http://docs.python.org/3/library/stdtypes.html#mapping-types-dict):

```python
c = DictCache('foo')
c['bar'] = 'b'
c['che'] = 'c'
for key, val in c.items():
  print key, val # will print "bar b" and then "che c"
```


#### SetCache

This caching object acts more or less like a Python [set](http://docs.python.org/2/library/stdtypes.html#set):

```python
c = SetCache('foo')
c.add('bar')
c.add('che')
print 'che' in c # True
```


#### SortedSetCache

This caching object acts more or less like a Python [set](http://docs.python.org/2/library/stdtypes.html#set) but has some changes:

* The `add()` method takes a tuple `(score, elem)`
* The `pop()` method will pop off the lowest score from the set, and pops a tuple `(score, elem)`
* An `rpop()` method allows you to pop the highest score from the set.
* Iterating through the set results in tuples of `(score, elem)`, not just elem like in a normal set or the `SetCache`.

```python
c = SortedSetCache('foo')
c.add((1, 'bar'))
c.add((10, 'che'))
print 'che' in c # True
print c.pop() # (1, bar)
```


#### SentinelCache

Handy for gated access:

```python
c = SentinelCache('foo')

if not c:
    print("sentinel value isn't set so do this")

if not c:
    print("sentinel value is now set so this will never run")
```


### Decorator

Caches exposes a decorator to make caching the return value of a function easy. This only works for `Cache` derived caching.

The `cached` decorator can accept a caching class and also a key function (similar to the python [built-in `sorted()` function](http://docs.python.org/2/library/functions.html#sorted) key argument), except caches key argument returns a list that can be passed to the constructor of the caching class as `*args`.

```python
import functools
from caches import Cache

@Cache.cached(key="some_cache_key")
def foo(*args):
    return functools.reduce(lambda x, y: x+y, args)

foo(1, 2) # will compute the value and cache the return value
foo(1, 2) # return value from cache

foo(1, 2, 3) # uh-oh, wrong value, our key was too static
```

Let's try again, this time with a dynamic key

```python
@Cache.cached(key=lambda *args: args)
def foo(*args):
    return functools.reduce(lambda x, y: x+y, args)

foo(1, 2) # compute and cache, key func returned [1, 2]
foo(1, 2) # grabbed from cache
foo(1, 2, 3) # compute and cache because our key func returned [1, 2, 3]
```

What about custom caches classes?

```python
class CustomCache(Cache): pass

@CustomCache.cached(key=lambda *args: args)
def foo(*args):
    return functools.reduce(lambda x, y: x+y, args)
```


## Install

Use pip from pypi:

    pip install caches

or from source using pip:

    pip install -U "git+https://github.com/jaymon/caches#egg=caches"



## License

MIT

## Other links

* [redis_collections module](https://github.com/redis-collections/redis-collections) - If you need broader/deeper support for python standard types like dict and set then check out this project. Prior to 2.0.0 Caches had a dependency on this module.
* [Dogpile](http://dogpilecache.readthedocs.org/en/latest/usage.html)

