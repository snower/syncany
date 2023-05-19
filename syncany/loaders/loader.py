# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import types
import re
from collections import defaultdict, deque
from ..valuers.valuer import ContextRunner, ContextDataer


class KeyMatcher(object):
    def __init__(self, matcher, valuer):
        if isinstance(matcher, str):
            self.matcher = re.compile(matcher)
        else:
            self.matcher = matcher
        self.valuer = valuer
        self.key_events = []

    def clone(self):
        key_matcher = self.__class__(self.matcher, self.clone_valuer())
        key_matcher.key_events = self.key_events
        return key_matcher

    def match(self, key):
        return self.matcher.match(key)

    def clone_valuer(self):
        return self.valuer.clone()

    def create_key(self, key):
        valuer = self.clone_valuer()
        valuer.key = key
        for key_event in self.key_events:
            key_event(key, valuer)
        return valuer

    def add_key_event(self, event):
        self.key_events.append(event)


class Loader(object):
    def __init__(self, primary_keys, valuer_type=0, **kwargs):
        self.primary_keys = primary_keys
        self.valuer_type = valuer_type
        self.schema = {}
        self.filters = []
        self.orders = []
        self.intercepts = []
        self.current_cursor = None
        self.key_matchers = []
        self.datas = []
        self.loaded = False
        self.geted = False
        self.loader_state = defaultdict(int)

    def clone(self):
        loader = self.__class__(self.primary_keys, self.valuer_type)
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
        loader.orders = [order for order in self.orders]
        loader.intercepts = [intercept.clone() for intercept in self.intercepts]
        loader.key_matchers = [matcher.clone() for matcher in self.key_matchers]
        return loader

    def add_valuer(self, name, valuer):
        self.schema[name] = valuer

    def add_intercept(self, intercept):
        self.intercepts.append(intercept)

    def add_key_matcher(self, matcher, valuer):
        matcher = KeyMatcher(matcher, valuer)
        self.key_matchers.append(matcher)
        return matcher

    def get_data_primary_key(self, data):
        if len(self.primary_keys) == 1:
            return data.get(self.primary_keys[0], '')
        return tuple(data.get(pk, '') for pk in self.primary_keys)

    def next(self):
        if not self.loaded:
            return True
        return False

    def is_dynamic_schema(self):
        return False

    def is_streaming(self):
        return None

    def set_streaming(self, is_streaming=None):
        pass

    def load(self, timeout=None):
        self.loaded = True

    def get(self):
        if self.geted:
            return self.datas
        if not self.loaded:
            self.load()

        datas, self.datas = deque(self.datas), []
        if not self.valuer_type:
            while datas:
                data, odata = datas.popleft(), {}
                if isinstance(data, ContextDataer):
                    data.use_values()
                    for name, valuer in self.schema.items():
                        odata[name] = valuer.get()
                else:
                    for name, valuer in self.schema.items():
                        if name not in data or not isinstance(data[name], ContextRunner):
                            odata[name] = valuer.fill_get(data)
                        else:
                            odata[name] = data[name].get()
                if self.intercepts and self.check_intercepts(odata):
                    continue
                self.datas.append(odata)
            self.geted = True
            return self.datas

        while datas:
            data, odata, oyields, ofuncs = datas.popleft(), {}, {}, {}
            if isinstance(data, ContextDataer):
                data.use_values()
                for name, valuer in self.schema.items():
                    value = valuer.get()
                    if isinstance(value, types.FunctionType):
                        ofuncs[name] = value
                        odata[name] = None
                        continue
                    if isinstance(value, types.GeneratorType):
                        oyields[name] = value
                        odata[name] = None
                        continue
                    odata[name] = value
            else:
                for name, valuer in self.schema.items():
                    if name not in data or not isinstance(data[name], ContextRunner):
                        value = valuer.fill_get(data)
                    else:
                        value = data[name].get()
                    if isinstance(value, types.FunctionType):
                        ofuncs[name] = value
                        odata[name] = None
                        continue
                    if isinstance(value, types.GeneratorType):
                        oyields[name] = value
                        odata[name] = None
                        continue
                    odata[name] = value

            if oyields:
                while oyields:
                    oyield_data = {name: value for name, value in odata.items()}
                    has_oyield_data = False
                    for name, oyield in tuple(oyields.items()):
                        try:
                            oyield_data[name] = oyield.send(oyield_data)
                            has_oyield_data = True
                        except StopIteration as e:
                            if e.value is not None:
                                oyield_data[name] = e.value
                                has_oyield_data = True
                            oyields.pop(name)
                    if has_oyield_data:
                        if self.intercepts and self.check_intercepts(oyield_data):
                            continue
                        if ofuncs:
                            has_func_data = False
                            for name, ofunc in ofuncs.items():
                                try:
                                    oyield_data[name] = ofunc(oyield_data)
                                    has_func_data = True
                                except StopIteration:
                                    continue
                            if has_func_data:
                                self.datas.append(oyield_data)
                        else:
                            self.datas.append(oyield_data)
            else:
                if self.intercepts and self.check_intercepts(odata):
                    continue
                if ofuncs:
                    has_func_data = False
                    for name, ofunc in ofuncs.items():
                        try:
                            odata[name] = ofunc(odata)
                            has_func_data = True
                        except StopIteration:
                            continue
                    if has_func_data:
                        self.datas.append(odata)
                else:
                    self.datas.append(odata)
        self.geted = True
        return self.datas

    def check_intercepts(self, data):
        for intercept in self.intercepts:
            intercept_result = intercept.fill_get(data)
            if intercept_result is not None and not intercept_result:
                return True
        return False

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
        for filter in self.filters:
            if filter[1] == "limit":
                filter[2] = value
                return
        self.add_filter(None, "limit", value)

    def filter_cursor(self, last_data, offset, count):
        self.current_cursor = (last_data, offset, count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def statistics(self):
        return {
            "rows": len(self.datas)
        }