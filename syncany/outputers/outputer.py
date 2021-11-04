# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from collections import defaultdict

class Outputer(object):
    def __init__(self, primary_keys, **kwargs):
        self.primary_keys = primary_keys
        self.schema = {}
        self.filters = []
        self.current_cursor = None
        self.load_datas = []
        self.datas = []
        self.outputer_state = defaultdict(int)

    def clone(self):
        outputer = self.__class__(self.primary_keys)
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        outputer.schema = schema
        outputer.filters = [filter for filter in self.filters]
        return outputer

    def add_valuer(self, name, valuer):
        self.schema[name] = valuer

    def get_data_primary_key(self, data):
        if len(self.primary_keys) == 1:
            return data.get(self.primary_keys[0], '')
        return ".".join([data.get(pk, '') for pk in self.primary_keys])

    def store(self, datas):
        self.datas = datas

    def add_filter(self, key, exp, value):
        self.filters.append([key, exp, value])

    def filter_gt(self, key, value):
        for filter in self.filters:
            if key == filter[0] and "gt" == filter[1]:
                filter[2] = value
                return

        self.add_filter(key, "gt", value)

    def filter_gte(self, key, value):
        for filter in self.filters:
            if key == filter[0] and "gte" == filter[1]:
                filter[2] = value
                return

        self.add_filter(key, "gte", value)

    def filter_lt(self, key, value):
        for filter in self.filters:
            if key == filter[0] and "lt" == filter[1]:
                filter[2] = value
                return

        self.add_filter(key, "lt", value)

    def filter_lte(self, key, value):
        for filter in self.filters:
            if key == filter[0] and "lte" == filter[1]:
                filter[2] = value
                return

        self.add_filter(key, "lte", value)

    def filter_eq(self, key, value):
        self.add_filter(key, "eq", value)

    def filter_ne(self, key, value):
        self.add_filter(key, "ne", value)

    def filter_in(self, key, value):
        self.add_filter(key, "in", value)

    def filter_cursor(self, last_data, offset, count):
        self.current_cursor = (last_data, offset, count)

    def statistics(self):
        return {
            "load_rows": len(self.load_datas),
            "rows": len(self.datas)
        }