# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import types
import re
from collections import OrderedDict

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
    def __init__(self, primary_keys, is_yield=False):
        self.primary_keys = primary_keys
        self.is_yield = is_yield
        self.schema = OrderedDict()
        self.filters = []
        self.key_matchers = []
        self.datas = []
        self.data_keys = {}
        self.loaded = False

    def clone(self):
        loader = self.__class__(self.primary_keys)
        schema = OrderedDict()
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
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

    def load(self):
        self.loaded = True

    def __getitem__(self, item):
        if not self.loaded:
            self.load()

        if isinstance(item, str):
            return self.data_keys[item]
        return self.datas[item]

    def __iter__(self):
        if not self.loaded:
            self.load()

        return self.datas

    def get(self):
        if not self.loaded:
            self.load()

        datas = []
        if not self.is_yield:
            for data in self.datas:
                odata = OrderedDict()
                for name, valuer in self.schema.items():
                    if name not in data:
                        odata[name] = valuer.get()
                        continue
                    odata[name] = data[name].get()
                datas.append(odata)
            return datas

        oyields = OrderedDict()
        for data in self.datas:
            odata = OrderedDict()
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
                    oyield_data = OrderedDict()
                    for name, oyield in list(oyields.items()):
                        try:
                            oyield_data[name] = oyield.send(oyield_data)
                        except StopIteration:
                            oyields.pop(name)
                    if oyield_data:
                        for name, value in odata.items():
                            if name not in oyield_data:
                                oyield_data[name] = value
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

    def statistics(self):
        return {
            "rows": len(self.datas)
        }