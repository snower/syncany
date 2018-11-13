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
        if data is None:
            return self

        if not self.key:
            return self

        if self.key == "*":
            self.value = data
            return self

        if self.key not in data:
            keys = self.key.split(".")
            for key in keys:
                if isinstance(data, (list, tuple, set)):
                    if key[0] != ":":
                        data = [value[key] for value in data if key in value]
                        continue

                    slices = key.split(":")
                    try: start = int(slices[1])
                    except: start = 0

                    if len(slices) <= 2:
                        data = data[start]
                        continue

                    try: end = int(slices[1])
                    except: end = len(data)

                    try: step = int(slices[1])
                    except: step = 1

                    data = data[start: end: step]

                elif isinstance(data, dict):
                    if key[0] == ":":
                        continue
                    elif key not in data:
                        break

                    data = data[key]

                else:
                    break

            self.value = data
            if self.filter:
                if isinstance(self.value, (list, tuple, set)):
                    values = []
                    for value in self.value:
                        values.append(self.filter.filter(value))
                    self.value = values
                else:
                    self.value = self.filter.filter(self.value)
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
        return False