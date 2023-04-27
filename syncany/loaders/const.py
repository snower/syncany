# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .loader import Loader


class ConstLoader(Loader):
    def __init__(self, datas, *args, **kwargs):
        super(ConstLoader, self).__init__(*args, **kwargs)

        self.const_datas = datas
        self.last_data = datas[-1] if datas else None

    def clone(self):
        loader = self.__class__(self.const_datas, self.primary_keys, self.valuer_type)
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
        loader.orders = [order for order in self.orders]
        loader.intercepts = [intercept.clone() for intercept in self.intercepts]
        loader.key_matchers = [matcher.clone() for matcher in self.key_matchers]
        return loader

    def load(self, timeout=None):
        if self.loaded:
            return
        self.datas = self.const_datas[:]
        self.loaded = True