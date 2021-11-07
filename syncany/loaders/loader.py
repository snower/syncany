# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import types
import re
from collections import defaultdict

class KeyMatcher(object):
    def __init__(self, matcher, valuer):
        if isinstance(matcher, str):
            self.matcher = re.compile(matcher)
        else:
            self.matcher = matcher
        self.valuer = valuer

    def clone(self):
        return self.__class__(self.matcher, self.clone_valuer())

    def match(self, key):
        return self.matcher.match(key)

    def clone_valuer(self):
        return self.valuer.clone()

class Loader(object):
    def __init__(self, primary_keys, is_yield=False, **kwargs):
        self.primary_keys = primary_keys
        self.is_yield = is_yield
        self.schema = {}
        self.filters = []
        self.orders = []
        self.current_cursor = None
        self.key_matchers = []
        self.datas = []
        self.loaded = False
        self.loader_state = defaultdict(int)

    def clone(self):
        loader = self.__class__(self.primary_keys, self.is_yield)
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
        loader.orders = [order for order in self.orders]
        loader.key_matchers = [matcher.clone() for matcher in self.key_matchers]
        return loader

    def add_valuer(self, name, valuer):
        self.schema[name] = valuer

    def add_key_matcher(self, matcher, valuer):
        matcher = KeyMatcher(matcher, valuer)
        self.key_matchers.append(matcher)
        return matcher

    def get_data_primary_key(self, data):
        if len(self.primary_keys) == 1:
            return data.get(self.primary_keys[0], '')
        return ".".join([data.get(pk, '') for pk in self.primary_keys])

    def next(self):
        if not self.loaded:
            return True
        return False

    def load(self, timeout=None):
        self.loaded = True

    def get(self):
        if not self.loaded:
            self.load()

        datas = []
        if not self.is_yield:
            for data in self.datas:
                odata = {}
                for name, valuer in self.schema.items():
                    if name not in data:
                        odata[name] = valuer.get()
                        continue
                    odata[name] = data[name].get()
                datas.append(odata)
            return datas

        oyields = {}
        for data in self.datas:
            odata = {}
            for name, valuer in self.schema.items():
                if name not in data:
                    odata[name] = valuer.get()
                    continue
                value = data[name].get()
                if isinstance(value, types.GeneratorType):
                    oyields[name] = value
                    final_filter = valuer.get_final_filter()
                    if final_filter:
                        odata[name] = final_filter.filter(None)
                    else:
                        odata[name] = None
                    continue
                odata[name] = value

            if oyields:
                while oyields:
                    oyield_data = {}
                    for name, value in odata.items():
                        oyield_data[name] = value
                    has_oyield_data = False
                    for name, oyield in list(oyields.items()):
                        try:
                            oyield_data[name] = oyield.send(oyield_data)
                            has_oyield_data = True
                        except StopIteration:
                            oyields.pop(name)
                    if has_oyield_data:
                        datas.append(oyield_data)
            else:
                datas.append(odata)
        return datas

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

    def filter_limit(self, value):
        self.add_filter(None, "limit", value)

    def filter_cursor(self, last_data, offset, count):
        self.current_cursor = (last_data, offset, count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def statistics(self):
        return {
            "rows": len(self.datas)
        }