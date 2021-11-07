# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .loader import Loader

class ConstLoader(Loader):
    def __init__(self, datas, *args, **kwargs):
        super(ConstLoader, self).__init__(*args, **kwargs)

        self.const_datas = datas

    def clone(self):
        loader = self.__class__(self.const_datas, self.primary_keys, self.is_yield)
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
        loader.orders = [order for order in self.orders]
        loader.key_matchers = [matcher.clone() for matcher in self.key_matchers]
        return loader

    def load(self, timeout=None):
        if self.loaded:
            return

        for data in self.const_datas:
            values = {}
            for key, field in self.schema.items():
                values[key] = field.clone().fill(data)
            self.datas.append(values)

        self.loaded = True