# Caches

A Python caching library that gives a similar interface to standard Python data structures like Dict and Set but is backed by redis.

Caches was lovingly crafted for [First Opinion](http://firstopinion.co).


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

All caches caching classes have a similar interface, they take the passed in constructor `*args` and concat them to create a key:

```python
c = KeyCache('foo', 'bar', 'che')
print c.key # foo.bar.che
```

If you would like to init your cache object with a value, use the `data` `**kwarg`:

```python
c = KeyCache('foo', data="boom!")
print c.key # foo
print c # "boom!"
```

Each caches base caching class is meant to be extended so you can set some parameters:

* **serialize** -- boolean -- True if you want all values pickled, False if you don't (ie, you're caching ints or strings or something).

* **prefix** -- string -- This will be prepended to the key args you pass into the constructor.

* **ttl** -- integer -- time to live, how many seconds to cache the value. Set to like 2 hours by default, 0 means live forevor.

* **connection_name** -- string -- if you have more than one caches dsn then you can use this to set the name of the connection you want (the name of the connection is the `#connection_name` fragment of a dsn url).

```python
class MyIntCache(KeyCache):
  serialize = False # don't bother to serialize values since we're storing ints
  prefix = "MyIntCache" # every key will have this prefix, change to invalidate all currently cached values
  ttl = 7200 # store each int for 2 hours
```

### Cache Classes


#### KeyCache

This is the traditional caching object, it sets a value into a key:

```python
c = KeyCache('foo')
c.data = 5 # cache 5
c += 10 # increment 5 by 10, store 15 in the cache

c.clear()
print c # None
```


#### DictCache

This caching object acts more or less like a Python [dictionary](http://docs.python.org/2/library/stdtypes.html#mapping-types-dict):

```python
c = DictCache('foo')
c['bar'] = 'b'
c['che'] = 'c'
for key, val in c.iteritems():
  print key, val # will print bar b and then che c
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

* The add() method can take a score value
* The pop() method will pop off the lowest score from the set, and pops a tuple: (elem, score)
* An rpop() method allows you to pop the highest score from the set.
* Iterating through the set results in tuples of (elem, score), not just elem like in a normal set or the `SetCache`.
* The chunk(limit, offset) and rchunk(limit, offset) methods will work through sections of the list working either forwards or backwards.

```python
c = SortedSetCache('foo')
c.add('bar', 1)
c.add('che', 10)
print 'che' in c # True
print c.pop() # (bar, 1)
```


#### CounterCache

This caching object acts more or less like a Python [collections.Counter](http://docs.python.org/2/library/collections.html#collections.Counter):

```python
c = CounterCache('foo')
c['bar'] = 5
c['bar'] += 5

print c['bar'] # 10
```


### Decorator

Caches exposes a decorator to make caching the return value of a function easy. This only works for `KeyCache` derived caching.

The `cached` decorator can accept a caching class and also a key function (similar to the python [built-in `sorted()` function](http://docs.python.org/2/library/functions.html#sorted) key argument), except caches key argument returns a list that can be passed to the constructor of the caching class as `*args`.

```python
from caches import KeyCache

@KeyCache.cached(key="some_cache_key")
def foo(*args):
    return reduce(lambda x, y: x+y, args)

foo(1, 2) # will compute the value and cache the return value
foo(1, 2) # return value from cache

foo(1, 2, 3) # uh-oh, wrong value, our key was too static
```

Let's try again, this time with a dynamic key

```python
@KeyCache.cached(key=lambda *args: args)
def foo(*args):
    return reduce(lambda x, y: x+y, args)

foo(1, 2) # compute and cache, key func returned [1, 2]
foo(1, 2) # grabbed from cache
foo(1, 2, 3) # compute and cache because our key func returned [1, 2, 3]
```

What about custom caches classes?

```python
class CustomCache(KeyCache): pass

@CustomCache.cached(key=lambda *args: args)
def foo(*args):
    return reduce(lambda x, y: x+y, args)
```


## Install

Use pip from pypi:

    pip install caches

or from source using pip:

    pip install git+https://github.com/firstopinion/caches#egg=caches


## Acknowledgements

Caches uses the very cool [redis_collections module](https://redis-collections.readthedocs.org/en/latest/).

Some of the interface is inspired from a module that [Ryan Johnson](https://github.com/bismark) wrote for Undrip.


## License

MIT

## Other links

[Dogpile](http://dogpilecache.readthedocs.org/en/latest/usage.html)

