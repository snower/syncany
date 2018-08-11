# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

class Valuer(object):
    def __init__(self, key, filter = None):
        self.key = key
        self.filter = filter
        self.value = None

        if self.filter:
            self.value = self.filter.filter(self.value)

    def clone(self):
        return self.__class__(self.key, self.filter)

    def fill(self, data):
        if self.key not in data:
            keys = self.key.split(".")
            for key in keys:
                if not isinstance(data, dict) or key not in data:
                    return self
                data = data[key]
            self.value = data
            if self.filter:
                self.value = self.filter.filter(self.value)
        else:
            self.value = data[self.key]
            if self.filter:
                self.value = self.filter.filter(self.value)
        return self

    def get(self):
        return self.value

    def childs(self):
        return []

    def get_fields(self):
        return []