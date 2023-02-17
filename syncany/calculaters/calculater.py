# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

class Calculater(object):
    def __init__(self, name, *args):
        self.type_cls = None
        self.name = name
        self.args = args

    def format_type(self, value):
        if value is None:
            return value
        if self.type_cls is None:
            self.type_cls = type(value)
            return value
        if isinstance(value, self.type_cls):
            return value
        return self.type_cls(value)

    def calculate(self):
        if not self.args:
            return None

        if len(self.args) == 1:
            return self.args[0]

        return self.args

    def get_key_value(self, key, data):
        keys = key.split(".")
        for key in keys:
            if isinstance(data, list):
                if key[0] != ":":
                    return data

                slices = key.split(":")
                try:
                    start = int(slices[1])
                except:
                    start = 0

                if len(slices) <= 2:
                    data = data[start]
                    continue

                try:
                    end = int(slices[1])
                except:
                    end = len(data)

                try:
                    step = int(slices[1])
                except:
                    step = 1

                data = data[start: end: step]

            elif isinstance(data, dict):
                if key not in data:
                    return data
                data = data[key]

        return data