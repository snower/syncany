# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import datetime
from ..errors import SyncanyException
from ..utils import ensure_timezone


class LoadAllFieldsException(SyncanyException):
    pass


def get_key(data, key, dot_keys):
    if key in data:
        return data[key], 1
    if not dot_keys or len(dot_keys) <= 1:
        return None, 1
    for i in range(1, len(dot_keys)):
        dot_key = ".".join(dot_keys[:i + 1])
        if dot_key in data:
            return data[dot_key], i + 1
    return None, 1

def dict_key(key, dot_keys=None):
    def _(data):
        if isinstance(data, dict):
            return get_key(data, key, dot_keys)
        if isinstance(data, list):
            rdata, rindex = [], len(dot_keys)
            for value in data:
                if not isinstance(value, dict):
                    if not hasattr(value, key):
                        continue
                    value, vindex = getattr(value, key), 1
                else:
                    value, vindex = get_key(value, key, dot_keys)
                if value is None:
                    continue
                rdata.append(value)
                rindex = min(rindex, vindex)
            return rdata, rindex
        if hasattr(data, key):
            return getattr(data, key), 1
        return None, 1
    return _

def list_key(key):
    def _(data):
        if isinstance(data, list) and key < len(data):
            return data[key], 1
        if isinstance(data, dict):
            colon_key = ":%d" % key
            if colon_key in data:
                return data[colon_key], 1
            if key == 0:
                return data, 1
            return None, 1
        return None, 1
    return _

def slice_key(key):
    def _(data):
        if isinstance(data, list):
            if len(key) == 1:
                return data[key[0]:], 1
            if len(key) == 2:
                return data[key[0]: key[1]], 1
            return data[key[0]: key[1]: key[2]], 1
        if isinstance(data, dict):
            colon_key = ":" + ":".join([str(k) for k in key])
            if colon_key in data:
                return data[colon_key], 1
            if key and key[0] == 0:
                return data, 1
            return None, 1
        return None, 1
    return _


class ContextRunner(object):
    def __init__(self, contexter, valuer, values=None):
        self.contexter = contexter
        self.valuer = valuer
        self.values = {} if values is None else values

    def fill(self, data):
        self.contexter.values = self.values
        self.valuer.fill(data)
        return self

    def get(self):
        self.contexter.values = self.values
        return self.valuer.get()


class ContextDataer(object):
    def __init__(self, contexter):
        self.contexter = contexter
        self.values = {}

    def fill(self, valuer, data):
        return valuer.fill(data)

    def get(self, valuer):
        return valuer.get()

    def use_values(self):
        self.contexter.values = self.values
        return self

    def __enter__(self):
        self.contexter.values = self.values

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class Contexter(object):
    def __init__(self):
        self.values = {}

    def create_runner(self, valuer, values=None):
        return ContextRunner(self, valuer, values)


class Valuer(object):
    KEY_GETTER_CACHES = {}
    value = None

    def __init__(self, key, filter=None, from_valuer=None):
        self.key = key
        self.filter = filter
        if from_valuer is None:
            self.new_init()
        else:
            self.clone_init(from_valuer)

    def new_init(self):
        self.key_getters = []

    def clone_init(self, from_valuer):
        self.key_getters = from_valuer.key_getters

    def parse_key(self):
        if self.key in self.KEY_GETTER_CACHES:
            self.key_getters = self.KEY_GETTER_CACHES[self.key]
            return

        keys = self.key.split(".")
        for i in range(len(keys)):
            key = keys[i]
            if key[:1] == ":":
                slices = []
                for slice in key[1:].split(":"):
                    try:
                        slices.append(int(slice))
                    except:
                        slices.append(0 if not slices else None)
                        if len(slices) == 3:
                            break

                if len(slices) == 1:
                    self.key_getters.append(list_key(slices[0]))
                else:
                    slices = [slice for slice in slices if slice is not None]
                    self.key_getters.append(slice_key(slices))
            else:
                self.key_getters.append(dict_key(key, keys[i:]))
        if len(self.KEY_GETTER_CACHES) > 1024:
            cache_keys = list(self.KEY_GETTER_CACHES.keys())
            for cache_key in cache_keys[:len(cache_keys) - 512]:
                self.KEY_GETTER_CACHES.pop(cache_key, None)
        self.KEY_GETTER_CACHES[self.key] = self.key_getters

    def clone(self, contexter=None):
        if contexter is not None:
            return ContextValuer(self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextValuer):
            return ContextValuer(self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.key, self.filter, from_valuer=self)

    def reinit(self):
        self.value = None
        for valuer in self.childs():
            valuer.reinit()
        return self

    def fill(self, data):
        if isinstance(data, dict) and self.key in data:
            self.value = self.do_filter(data[self.key])
            return self
        if data is None or not self.key:
            self.value = self.do_filter(None)
            return self
        if self.key == "*":
            self.value = self.do_filter(data)
            return self

        if not self.key_getters:
            if self.key in self.KEY_GETTER_CACHES:
                self.key_getters = self.KEY_GETTER_CACHES[self.key]
            else:
                self.parse_key()
        try:
            key_getter_index, key_getter_len = 0, len(self.key_getters)
            while key_getter_index < key_getter_len:
                data, index = self.key_getters[key_getter_index](data)
                if data is None:
                    break
                key_getter_index += index
            self.value = self.do_filter(data)
        except:
            self.value = self.do_filter(None)
        return self

    def get(self):
        return self.value

    def reset(self):
        for valuer in self.childs():
            valuer.reset()

    def do_filter(self, value):
        if not self.filter:
            if isinstance(value, datetime.datetime):
                value = ensure_timezone(value)
            return value
        return self.filter.filter(value)

    def childs(self):
        return []

    def get_fields(self):
        return []

    def get_final_filter(self):
        return self.filter

    def require_loaded(self):
        for child in self.childs():
            if child.require_loaded():
                return True
        return False


class ContextValuer(Valuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextValuer, self).__init__(*args, **kwargs)

    @property
    def value(self):
        try:
            return self.contexter.values[self.value_context_id]
        except KeyError:
            return None

    @value.setter
    def value(self, v):
        if v is None:
            if self.value_context_id in self.contexter.values:
                self.contexter.values.pop(self.value_context_id)
            return
        self.contexter.values[self.value_context_id] = v
