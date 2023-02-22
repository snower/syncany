# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import datetime
from ..filters import DateTimeFilter, DateFilter, TimeFilter


class Calculater(object):
    def __init__(self, name, *args):
        self.type_cls = None
        self.name = name
        self.args = args

    def format_type(self, value):
        if value is None:
            if self.type_cls:
                if issubclass(self.type_cls, str):
                    return ""
                if issubclass(self.type_cls, (int, float)):
                    return 0
            return value
        if self.type_cls is None:
            self.type_cls = type(value)
            return value
        if isinstance(value, self.type_cls):
            return value
        if issubclass(self.type_cls, datetime.date):
            if issubclass(self.type_cls, datetime.datetime):
                return DateTimeFilter().filter(value)
            return DateFilter().filter(value)
        if issubclass(self.type_cls, datetime.time):
            return TimeFilter().filter(value)
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