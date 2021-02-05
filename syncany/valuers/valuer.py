# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from ..errors import SyncanyException

class LoadAllFieldsException(SyncanyException):
    pass

def dict_key(key):
    def _(data):
        return data[key]
    return _

def list_key(key):
    def _(data):
        return data[key]
    return _

def slice_key(key):
    def _(data):
        if len(key) == 1:
            return data[key[0]:]
        if len(key) == 2:
            return data[key[0]: key[1]]
        return data[key[0]: key[1]: key[2]]
    return _

class Valuer(object):
    KEY_GETTER_CACHES = {}

    def __init__(self, key, filter=None):
        self.key = key
        self.filter = filter
        self.value = None
        self.key_getters = []

        if self.filter:
            self.value = self.filter.filter(self.value)

    def parse_key(self):
        if self.key in self.KEY_GETTER_CACHES:
            self.key_getters = self.KEY_GETTER_CACHES[self.key]
            return

        for key in self.key.split("."):
            if key[:1] == ":":
                slices = []
                for slice in key.split(":"):
                    try:
                        slices.append(int(slice))
                    except:
                        pass
                if len(slices) == 1:
                    self.key_getters.append(list_key(slices[0]))
                else:
                    self.key_getters.append(slice_key(slices))
            else:
                self.key_getters.append(dict_key(key))
        self.KEY_GETTER_CACHES[self.key] = self.key_getters

    def clone(self):
        return self.__class__(self.key, self.filter)

    def fill(self, data):
        if data is None or not self.key:
            if self.filter:
                self.value = self.filter.filter(self.value)
            return self

        if self.key == "*" or not isinstance(data, (dict, list, tuple, set)):
            self.value = data
            if self.filter:
                self.value = self.filter.filter(self.value)
            return self

        if self.key not in data:
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
        else:
            self.value = data[self.key]

        if self.filter:
            if isinstance(self.value, (list, tuple, set)):
                values = []
                for value in self.value:
                    values.append(self.filter.filter(value))
                self.value = values
            else:
                self.value = self.filter.filter(self.value)
        return self

    def get(self):
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