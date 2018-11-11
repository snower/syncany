# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

class Calculater(object):
    def __init__(self, *args):
        self.args = args

    def calculate(self):
        if not self.args:
            return None

        if len(self.args) == 1:
            return self.args[0]

        return self.args

    def get_key_value(self, key, data):
        keys = key.split(".")
        for key in keys:
            if isinstance(data, (list, tuple, set)):
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