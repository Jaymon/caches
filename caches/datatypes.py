# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from .core import Cache, BaseCache



class ObjectCache(BaseCache):
    serialize = True

    def __setattr__(self, name, val):
        if name in ["_data", "data", "key"]:
            #self.__dict__[name] = val
            super(ObjectCache, self).__setattr__(name, val)

        else:
            data = self.data
            setattr(data, name, val)
            self.data = data

    def __delattr__(self, name):
        try:
            super(ObjectCache, self).__delattr__(name)
        except AttributeError:
            data = self.data
            delattr(data, name)
            self.data = data

    def __getattribute__(self, name):
        if name == "__class__":
            try:
                data = super(ObjectCache, self).__getattribute__("data")
                if data is not None:
                    ret = data.__class__

            except AttributeError:
                ret = super(ObjectCache, self).__getattribute__(name)

        elif name == "_data":
            ret = super(ObjectCache, self).__getattribute__(name)

        else:
            try:
                ret = super(ObjectCache, self).__getattribute__(name)
            except AttributeError:
                data = super(ObjectCache, self).__getattribute__("data")
                if data is not None:
                    ret = getattr(data, name) 

        return ret

        ret = None
        klass = super(ObjectCache, self).__getattribute__("__class__")
        origklass = super(ObjectCache, self).__getattribute__("__origclass__")

        if klass is not origklass:
            try:
                ret = super(ObjectCache, self).__getattribute__(name)
            #ret = getattr(self.__origclass__, name, None)
            except AttributeError:
                data = super(ObjectCache, self).__getattribute__("data")
                ret = getattr(data, name) 

        return ret



        ret = None
        klass = super(ObjectCache, self).__getattribute__("__class__")
        origklass = super(ObjectCache, self).__getattribute__("__origclass__")

        if klass is not origklass:
            try:
                ret = super(ObjectCache, self).__getattribute__(name)
            #ret = getattr(self.__origclass__, name, None)
            except AttributeError:
                data = super(ObjectCache, self).__getattribute__("object")
                ret = getattr(data, name) 

        return ret

#         if name == "__dict__":
#             return super(
# 
#             ret = super(Redis, self).__getattribute__(name)
# 
# 
#         if name == "_data":
#             raise AttributeError(name)
# 
#         else:
#             ret = None
#             if self.__class__ is not self.__origclass__:
#                 ret = getattr(self.__origclass__, name, None)
#             if not ret:
#                 ret = getattr(self.data, name) 
# 
#         return ret

    def __instancecheck__(self, instance):
        pout.v(instance)
        return False

    def __subclasscheck__(self, instance):
        pout.v(instance)
        return False

#     def _update(self, data, pipe=None):
#         p = pipe
#         exe = False
#         if pipe is None and self.ttl:
#             p = self.redis.pipeline()
#             exe = True
# 
#         super(Cache, self)._update(data, pipe=p)
# 
#         if p:
#             if self.ttl:
#                 p.expire(self.key, self.ttl)
#             if exe:
#                 p.execute()





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

