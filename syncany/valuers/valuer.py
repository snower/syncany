# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from ..errors import SyncanyException

class LoadAllFieldsException(SyncanyException):
    pass

def dict_key(key):
    def _(data):
        if isinstance(data, list):
            if len(data) > 1:
                return [d[key] for d in data if isinstance(d, dict) and key in d]
            if len(data) == 1:
                data = data[0]
        if isinstance(data, dict) and key in data:
            return data[key]
        return None
    return _

def list_key(key):
    def _(data):
        if isinstance(data, dict):
            if key == 0:
                return data
            return None
        if isinstance(data, list) and key < len(data):
            return data[key]
        return None
    return _

def slice_key(key):
    def _(data):
        if isinstance(data, dict):
            if key and key[0] == 0:
                return data
            return None
        if isinstance(data, list):
            if len(key) == 1:
                return data[key[0]:]
            if len(key) == 2:
                return data[key[0]: key[1]]
            return data[key[0]: key[1]: key[2]]
        return None
    return _

class Valuer(object):
    KEY_GETTER_CACHES = {}

    def __init__(self, key, filter=None, **state_kwargs):
        self.key = key
        self.filter = filter
        self.value = None
        self.key_getters = []

        if self.filter:
            self.value = self.filter.filter(self.value)

        if state_kwargs:
            for name, value in state_kwargs.items():
                setattr(self, name, value)
        else:
            self.init_valuer()

    def init_valuer(self):
        pass

    def parse_key(self):
        if self.key in self.KEY_GETTER_CACHES:
            self.key_getters = self.KEY_GETTER_CACHES[self.key]
            return

        for key in self.key.split("."):
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
                self.key_getters.append(dict_key(key))
        self.KEY_GETTER_CACHES[self.key] = self.key_getters

    def clone(self):
        return self.__class__(self.key, self.filter)

    def fill(self, data):
        if data is None or not self.key:
            self.do_filter(None)
            return self

        if self.key == "*" or not isinstance(data, (dict, list)):
            self.do_filter(data)
            return self

        if self.key in data:
            self.do_filter(data[self.key])
            return self

        if self.key in self.KEY_GETTER_CACHES:
            self.key_getters = self.KEY_GETTER_CACHES[self.key]
        else:
            self.parse_key()
        try:
            for getter in self.key_getters:
                data = getter(data)
            self.value = data
        except:
            self.value = None
        self.do_filter(self.value)
        return self

    def get(self):
        return self.value

    def do_filter(self, value):
        if not self.filter:
            self.value = value
            return value

        self.value = self.filter.filter(value)
        return self.value

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