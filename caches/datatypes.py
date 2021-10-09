# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from .core import Cache, BaseCache



class ImmutableCache(Cache):
    serialize = False
    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def default_val(cls):
        """Because immutable datatypes need to have a value, this needs to be set to
        something appropriate for each child class"""
        raise NotImplementedError()


class IntCache(ImmutableCache, int):

    @classmethod
    def default_val(cls):
        return 0

    @classmethod
    def incr_val(cls, key, delta, ttl=0):
        if ttl:
            with cls.interface.pipeline() as pipe:
                pipe.incr(key, delta)
                pipe.expire(key, ttl)
                res = pipe.execute()[0]

        else:
            res = cls.interface.incr(key, delta)
        return res

    @classmethod
    def decr_val(cls, key, delta, ttl=0):
        if ttl:
            with cls.interface.pipeline() as pipe:
                pipe.decr(key, delta)
                pipe.expire(key, ttl)
                res = pipe.execute()[0]

        else:
            res = cls.interface.decr(key, delta)
        return res

    def __new__(cls, *args, **kwargs):
        key, data = cls.new_val(*args, **kwargs)
        instance = super(IntCache, cls).__new__(IntCache, data)
        #instance = int.__new__(IntCache, kwargs.get("data", 0))
        instance.key = key
        return instance

    def __iadd__(self, other):
        v = self.incr_val(self.key, other, self.ttl)
        return type(self)(self.key, val=v)

    def __isub__(self, other):
        v = self.decr_val(self.key, other, self.ttl)
        return type(self)(self.key, val=v)

    def __iand__(self, other):
        data = self.__and__(other)
        return type(self)(self.key, data=data)

    def __idiv__(self, other):
        data = self.__div__(other)
        return type(self)(self.key, data=data)

    def __idivmod__(self, other):
        data = self.__divmod__(other)
        return type(self)(self.key, data=data)

    def __ifloordiv__(self, other):
        data = self.__floordiv__(other)
        return type(self)(self.key, data=data)

    def __ilshift__(self, other):
        data = self.__lshift__(other)
        return type(self)(self.key, data=data)

    def __imod__(self, other):
        data = self.__mod__(other)
        return type(self)(self.key, data=data)

    def __imul__(self, other):
        data = self.__mul__(other)
        return type(self)(self.key, data=data)

    def __ior__(self, other):
        data = self.__or__(other)
        return type(self)(self.key, data=data)

    def __ipow__(self, other):
        data = self.__pow__(other)
        return type(self)(self.key, data=data)

    def __itruediv__(self, other):
        data = self.__truediv__(other)
        return type(self)(self.key, data=data)

    def __ixor__(self, other):
        data = self.__xor__(other)
        return type(self)(self.key, data=data)

#     def __invert__(self):
#         return type(self)(self.key, data=super(IntCache, self).__invert__(other))
# 
#     def __neg__(self):
#         return type(self)(self.key, data=super(IntCache, self).__neg__())
# 
#     def __pos__(self):
#         return type(self)(self.key, data=super(IntCache, self).__pos__())
# 
#     def __trunc__(self):
#         return type(self)(self.key, data=super(IntCache, self).__trunc__())

#     def __radd__(self, other):
#         raise NotImplementedError()
# 
#     def __rand__(self, other):
#         raise NotImplementedError()
# 
#     def __rdivmod__(self, other):
#         raise NotImplementedError()
# 
#     def __rfloordiv__(self, other):
#         raise NotImplementedError()
# 
#     def __rlshift__(self, other):
#         raise NotImplementedError()
# 
#     def __rmod__(self, other):
#         raise NotImplementedError()
# 
#     def __rmul__(self, other):
#         raise NotImplementedError()
# 
#     def __ror__(self, other):
#         raise NotImplementedError()
# 
#     def __rpow__(self, other):
#         raise NotImplementedError()
# 
#     def __rrshift__(self, other):
#         raise NotImplementedError()
# 
#     def __rshift__(self, other):
#         raise NotImplementedError()
# 
#     def __rsub__(self, other):
#         raise NotImplementedError()
# 
#     def __rtruediv__(self, other):
#         raise NotImplementedError()
# 
#     def __rxor__(self, other):
#         raise NotImplementedError()


# class BoolCache(Cache, bool):
#     def __abs__(self):
#         raise NotImplementedError()
# 
#     def __add__(self, other):
#         raise NotImplementedError()
# 
#     def __and__(self, other):
#         raise NotImplementedError()
# 
#     def __cmp__(self, other):
#         raise NotImplementedError()
# 
#     def __coerce__(self, other):
#         raise NotImplementedError()
# 
#     def __delattr__(self, name):
#         raise NotImplementedError()
# 
#     def __div__(self, other):
#         raise NotImplementedError()
# 
#     def __divmod__(self, other):
#         raise NotImplementedError()
# 
#     def __float__(self):
#         raise NotImplementedError()
# 
#     def __floordiv__(self, other):
#         raise NotImplementedError()
# 
#     def __format__(self, formatstr):
#         raise NotImplementedError()
# 
#     def __getattribute__(self, name):
#         raise NotImplementedError()
# 
#     def __getnewargs__(self):
#         raise NotImplementedError()
# 
#     def __hash__(self):
#         raise NotImplementedError()
# 
#     def __hex__(self):
#         raise NotImplementedError()
# 
#     def __index__(self):
#         raise NotImplementedError()
# 
#     def __init__(self, *args, **kwargs):
#         raise NotImplementedError()
# 
#     def __int__(self):
#         raise NotImplementedError()
# 
#     def __invert__(self):
#         raise NotImplementedError()
# 
#     def __long__(self):
#         raise NotImplementedError()
# 
#     def __lshift__(self, other):
#         raise NotImplementedError()
# 
#     def __mod__(self, other):
#         raise NotImplementedError()
# 
#     def __mul__(self, other):
#         raise NotImplementedError()
# 
#     def __neg__(self):
#         raise NotImplementedError()
# 
#     def __new__(self, *args, **kwargs):
#         raise NotImplementedError()
# 
#     def __nonzero__(self):
#         raise NotImplementedError()
# 
#     def __oct__(self):
#         raise NotImplementedError()
# 
#     def __or__(self, other):
#         raise NotImplementedError()
# 
#     def __pos__(self):
#         raise NotImplementedError()
# 
#     def __pow__(self, other):
#         raise NotImplementedError()
# 
#     def __radd__(self, other):
#         raise NotImplementedError()
# 
#     def __rand__(self, other):
#         raise NotImplementedError()
# 
#     def __rdivmod__(self, other):
#         raise NotImplementedError()
# 
#     def __reduce__(self):
#         raise NotImplementedError()
# 
#     def __reduce_ex__(self):
#         raise NotImplementedError()
# 
#     def __repr__(self):
#         raise NotImplementedError()
# 
#     def __rfloordiv__(self, other):
#         raise NotImplementedError()
# 
#     def __rlshift__(self, other):
#         raise NotImplementedError()
# 
#     def __rmod__(self, other):
#         raise NotImplementedError()
# 
#     def __rmul__(self, other):
#         raise NotImplementedError()
# 
#     def __ror__(self, other):
#         raise NotImplementedError()
# 
#     def __rpow__(self, other):
#         raise NotImplementedError()
# 
#     def __rrshift__(self, other):
#         raise NotImplementedError()
# 
#     def __rshift__(self, other):
#         raise NotImplementedError()
# 
#     def __rsub__(self, other):
#         raise NotImplementedError()
# 
#     def __rtruediv__(self, other):
#         raise NotImplementedError()
# 
#     def __rxor__(self, other):
#         raise NotImplementedError()
# 
#     def __setattr__(self, name, value):
#         raise NotImplementedError()
# 
#     def __sizeof__(self):
#         raise NotImplementedError()
# 
#     def __str__(self):
#         raise NotImplementedError()
# 
#     def __sub__(self, other):
#         raise NotImplementedError()
# 
#     def __truediv__(self, other):
#         raise NotImplementedError()
# 
#     def __trunc__(self):
#         raise NotImplementedError()
# 
#     def __xor__(self, other):
#         raise NotImplementedError()


#class BoolCache(Cache, bool): pass

class StrCache(ImmutableCache, str):

    @classmethod
    def default_val(cls):
        return ""

    def __new__(cls, *args, **kwargs):
        key, data = cls.new_val(*args, **kwargs)
        instance = super(StrCache, cls).__new__(StrCache, data)
        instance.key = key
        return instance

    def __iadd__(self, other):
        data = self.__add__(other)
        return type(self)(self.key, data=data)

#     def __add__(self, other):
#         return type(self)(self.key, data=super(StrCache, self).__add__(other))

    def __imod__(self, other):
        data = self.__mod__(other)
        return type(self)(self.key, data=data)

    def __imul__(self, other):
        data = self.__mul__(other)
        return type(self)(self.key, data=data)

#     def __rmod__(self, other):
#         return type(self)(self.key, data=super(StrCache, self).__rmod__(other))
# 
#     def __rmul__(self, other):
#         return type(self)(self.key, data=super(StrCache, self).__rmul__(other))

