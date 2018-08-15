# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import re

class KeyMatcher(object):
    def __init__(self, matcher, valuer):
        if isinstance(matcher, str):
            self.matcher = re.compile(matcher)
        else:
            self.matcher = matcher
        self.valuer = valuer

    def match(self, key):
        return self.matcher.match(key)

    def clone_valuer(self):
        return self.valuer.clone()

class Loader(object):
    def __init__(self, primary_keys):
        self.primary_keys = primary_keys
        self.schema = {}
        self.filters = []
        self.key_matchers = []
        self.datas = []
        self.data_keys = {}
        self.loaded = False

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
        for data in self.datas:
            data = {key: valuer.get() for key, valuer in data.items()}
            datas.append(data)
        return datas

    def add_filter(self, key, exp, value):
        self.filters.append((key, exp, value))

    def filter_gt(self, key, value):
        self.add_filter(key, "gt", value)

    def filter_gte(self, key, value):
        self.add_filter(key, "gte", value)

    def filter_lt(self, key, value):
        self.add_filter(key, "lt", value)

    def filter_lte(self, key, value):
        self.add_filter(key, "lte", value)

    def filter_eq(self, key, value):
        self.add_filter(key, "eq", value)

    def filter_ne(self, key, value):
        self.add_filter(key, "ne", value)

    def filter_in(self, key, value):
        self.add_filter(key, "in", value)

    def statistics(self):
        return {
            "rows": len(self.datas)
        }