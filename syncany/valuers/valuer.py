# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from ..errors import SyncanyException


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


class ContexterInheritValues(dict):
    def __init__(self, inherit_values, *args, **kwargs):
        super(ContexterInheritValues, self).__init__(*args, **kwargs)
        self.inherit_values = inherit_values

    def __getitem__(self, item):
        try:
            return super(ContexterInheritValues, self).__getitem__(item)
        except KeyError:
            return self.inherit_values[item]


class Contexter(object):
    def __init__(self):
        self.values = {}

    def create_runner(self, valuer, values=None):
        return ContextRunner(self, valuer, values)

    def create_inherit_values(self, values):
        return ContexterInheritValues(values)


class Valuer(object):
    KEY_GETTER_CACHES = {}
    key = None
    filter = None
    value = None

    def __init__(self, key, filter=None, from_valuer=None):
        if key is not None:
            self.key = key
        if filter is not None:
            self.filter = filter
        if from_valuer is None:
            self.valuer_id = id(self)
            self.new_init()
        else:
            self.valuer_id = from_valuer.valuer_id
            self.clone_init(from_valuer)

    def update_key(self, key):
        if self.key is None and key is None:
            return
        self.key = key

    def new_init(self):
        self.key_getters = []

    def clone_init(self, from_valuer):
        self.key_getters = from_valuer.key_getters

    def parse_key(self):
        if self.key in self.KEY_GETTER_CACHES:
            self.key_getters = self.KEY_GETTER_CACHES[self.key]
            return
        if not self.key or self.key == "*":
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

    def mount_scoper(self, scoper=None, is_return_getter=False,**kwargs):
        for valuer in self.childs():
            valuer.mount_scoper(scoper=scoper, **kwargs)

    def clone(self, contexter=None, **kwargs):
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

    def fill_get(self, data):
        if isinstance(data, dict) and self.key in data:
            return self.do_filter(data[self.key])
        if data is None or not self.key:
            return self.do_filter(None)
        if self.key == "*":
            return self.do_filter(data)

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
            return self.do_filter(data)
        except:
            return self.do_filter(None)

    def reset(self):
        for valuer in self.childs():
            valuer.reset()

    def do_filter(self, value):
        self.do_filter = self.filter.filter if self.filter else lambda v: v
        return self.filter.filter(value) if self.filter else value

    def childs(self):
        return []

    def get_fields(self):
        return []

    def get_final_filter(self):
        return self.filter

    def require_loaded(self):
        if hasattr(self, "_cached_require_loaded"):
            return self._cached_require_loaded
        for child in self.childs():
            if child.require_loaded():
                setattr(self, "_cached_require_loaded", True)
                return True
        setattr(self, "_cached_require_loaded", False)
        return False

    def is_const(self):
        if hasattr(self, "_cached_is_const"):
            return self._cached_is_const
        for child in self.childs():
            if not child.is_const():
                setattr(self, "_cached_is_const", False)
                return False
        setattr(self, "_cached_is_const", True)
        return True

    def is_aggregate(self):
        if hasattr(self, "_cached_is_aggregate"):
            return self._cached_is_aggregate
        for child in self.childs():
            if child.is_aggregate():
                setattr(self, "_cached_is_aggregate", True)
                return True
        setattr(self, "_cached_is_aggregate", False)
        return False

    def is_yield(self):
        if hasattr(self, "_cached_is_yield"):
            return self._cached_is_yield
        for child in self.childs():
            if child.is_yield():
                setattr(self, "_cached_is_yield", True)
                return True
        setattr(self, "_cached_is_yield", False)
        return False


class ContextValuer(Valuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = id(self) * 10
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
