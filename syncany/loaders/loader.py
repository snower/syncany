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
        valuer.update_key(key)
        for key_event in self.key_events:
            key_event(key, valuer)
        return valuer

    def add_key_event(self, event):
        self.key_events.append(event)


class Loader(object):
    def __init__(self, primary_keys, valuer_type=0, **kwargs):
        self.primary_loader = None
        self.primary_keys = primary_keys
        self.valuer_type = valuer_type
        self.schema = {}
        self.filters = []
        self.orders = []
        self.predicate = None
        self.intercept = None
        self.current_cursor = None
        self.key_matchers = []
        self.datas = []
        self.loaded = False
        self.geted = False
        self.loader_state = defaultdict(int)

    def config(self, tasker):
        pass

    def clone(self):
        loader = self.__class__(self.primary_keys, self.valuer_type)
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
        loader.orders = [order for order in self.orders]
        loader.predicate = self.predicate
        loader.intercept = self.intercept
        loader.key_matchers = [matcher.clone() for matcher in self.key_matchers]
        return loader

    def add_valuer(self, name, valuer):
        self.schema[name] = valuer

    def update_predicate(self, predicate):
        self.predicate = predicate

    def update_intercept(self, intercept):
        self.intercept = intercept

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

        datas, self.datas = self.datas, []
        datas.reverse()
        if not self.valuer_type:
            while datas:
                data, odata = datas.pop(), {}
                if isinstance(data, ContextDataer):
                    data.contexter.values = data.values
                    if self.predicate is not None and not self.predicate.get():
                        continue
                    for name, valuer in self.schema.items():
                        odata[name] = valuer.get()
                else:
                    if isinstance(data, tuple):
                        if isinstance(data[0], ContextRunner):
                            if not data[0].get():
                                continue
                        elif self.predicate is not None and not self.predicate.fill_get(data[1]):
                            continue
                        data = data[1]
                    else:
                        if self.predicate is not None and not self.predicate.fill_get(data):
                            continue
                    for name, valuer in self.schema.items():
                        if name not in data or not isinstance(data[name], ContextRunner):
                            odata[name] = valuer.fill_get(data)
                        else:
                            odata[name] = data[name].get()
                if self.intercept is not None and not self.intercept.fill_get(odata):
                    continue
                self.datas.append(odata)
            self.geted = True
            return self.datas

        if self.valuer_type == 0x02:
            FunctionType, ofuncs = types.FunctionType, {}
            intercept_datas = deque() if self.intercept is not None else None
            while datas:
                data, odata, = datas.pop(), {}
                if isinstance(data, ContextDataer):
                    data.contexter.values = data.values
                    if self.predicate is not None and not self.predicate.get():
                        continue
                    for name, valuer in self.schema.items():
                        value = valuer.get()
                        if isinstance(value, FunctionType):
                            ofuncs[name] = value
                            odata[name] = None
                        else:
                            odata[name] = value
                else:
                    if isinstance(data, tuple):
                        if isinstance(data[0], ContextRunner):
                            if not data[0].get():
                                continue
                        elif self.predicate is not None and not self.predicate.fill_get(data[1]):
                            continue
                        data = data[1]
                    else:
                        if self.predicate is not None and not self.predicate.fill_get(data):
                            continue
                    for name, valuer in self.schema.items():
                        if name not in data or not isinstance(data[name], ContextRunner):
                            value = valuer.fill_get(data)
                        else:
                            value = data[name].get()
                        if isinstance(value, FunctionType):
                            ofuncs[name] = value
                            odata[name] = None
                        else:
                            odata[name] = value

                if ofuncs:
                    has_func_data = True
                    for name, ofunc in ofuncs.items():
                        try:
                            odata[name] = ofunc(odata)
                        except StopIteration:
                            has_func_data = False
                            continue
                    if has_func_data:
                        if self.intercept is None:
                            self.datas.append(odata)
                        else:
                            intercept_datas.append(odata)
                    ofuncs.clear()
                else:
                    if self.intercept is None:
                        self.datas.append(odata)
                    else:
                        intercept_datas.append(odata)

            while intercept_datas:
                data = intercept_datas.popleft()
                if self.intercept is not None and not self.intercept.fill_get(data):
                    continue
                self.datas.append(data)
            self.geted = True
            return self.datas

        if self.valuer_type in (0x01, 0x08, 0x03, 0x09, 0x0b):
            GeneratorType, GeneratorFunctionTypes = types.GeneratorType, (types.FunctionType, types.GeneratorType)
            oyield_generates, oyields, ofuncs = deque(), {}, {}
            intercept_datas = deque() if self.intercept is not None else None
            while datas:
                data, odata, predicate_yield = datas.pop(), {}, None
                if isinstance(data, ContextDataer):
                    data.contexter.values = data.values
                    if self.predicate is not None:
                        predicate_value = self.predicate.get()
                        if isinstance(predicate_value, GeneratorType):
                            predicate_yield = predicate_value
                        elif not predicate_value:
                            continue
                    for name, valuer in self.schema.items():
                        value = valuer.get()
                        if isinstance(value, GeneratorFunctionTypes):
                            if isinstance(value, GeneratorType):
                                oyields[name] = value
                            else:
                                ofuncs[name] = value
                            odata[name] = None
                        else:
                            odata[name] = value
                else:
                    if isinstance(data, tuple):
                        if isinstance(data[0], ContextRunner):
                            predicate_value = data[0].get()
                            if isinstance(predicate_value, GeneratorType):
                                predicate_yield = predicate_value
                            elif not predicate_value:
                                continue
                        elif self.predicate is not None and not self.predicate.fill_get(data[1]):
                            continue
                        data = data[1]
                    else:
                        if self.predicate is not None and not self.predicate.fill_get(data):
                            continue
                    for name, valuer in self.schema.items():
                        if name not in data or not isinstance(data[name], ContextRunner):
                            value = valuer.fill_get(data)
                        else:
                            value = data[name].get()
                        if isinstance(value, GeneratorFunctionTypes):
                            if isinstance(value, GeneratorType):
                                oyields[name] = value
                            else:
                                ofuncs[name] = value
                            odata[name] = None
                        else:
                            odata[name] = value

                if predicate_yield or oyields:
                    while True:
                        predicate_continue = False
                        while oyields:
                            oyield_odata, oyield_oyields, oyield_ofuncs, predicate_oyield = dict.copy(odata), {}, dict.copy(ofuncs), None
                            if predicate_yield:
                                try:
                                    predicate_value = predicate_yield.send(None)
                                    if isinstance(predicate_value, GeneratorType):
                                        predicate_oyield, predicate_continue = predicate_value, False
                                    else:
                                        predicate_continue = True if not predicate_value else False
                                except StopIteration:
                                    predicate_yield, predicate_continue = None, True

                            has_oyield_data = False
                            for name, oyield in tuple(oyields.items()):
                                try:
                                    value = oyield.send(None)
                                    if isinstance(value, GeneratorFunctionTypes):
                                        if isinstance(value, GeneratorType):
                                            oyield_oyields[name] = value
                                        else:
                                            oyield_ofuncs[name] = value
                                    else:
                                        oyield_odata[name] = value
                                    has_oyield_data = True
                                except StopIteration:
                                    oyields.pop(name)
                            if predicate_continue:
                                continue
                            if oyield_oyields:
                                oyield_generates.appendleft((odata, oyields, ofuncs, predicate_yield))
                                odata, oyields, ofuncs, predicate_yield = oyield_odata, oyield_oyields, oyield_ofuncs, predicate_oyield
                                continue

                            if has_oyield_data:
                                if oyield_ofuncs:
                                    has_func_data = True
                                    for name, ofunc in oyield_ofuncs.items():
                                        try:
                                            oyield_odata[name] = ofunc(oyield_odata)
                                        except StopIteration:
                                            has_func_data = False
                                            continue
                                    if has_func_data:
                                        if self.intercept is None:
                                            self.datas.append(oyield_odata)
                                        else:
                                            intercept_datas.append(oyield_odata)
                                    oyield_ofuncs.clear()
                                else:
                                    if self.intercept is None:
                                        self.datas.append(oyield_odata)
                                    else:
                                        intercept_datas.append(oyield_odata)

                        oyields.clear()
                        ofuncs.clear()
                        if not oyield_generates:
                            break
                        odata, oyields, ofuncs, predicate_yield = oyield_generates.popleft()
                else:
                    if ofuncs:
                        has_func_data = True
                        for name, ofunc in ofuncs.items():
                            try:
                                odata[name] = ofunc(odata)
                            except StopIteration:
                                has_func_data = False
                                continue
                        if has_func_data:
                            if self.intercept is None:
                                self.datas.append(odata)
                            else:
                                intercept_datas.append(odata)
                        ofuncs.clear()
                    else:
                        if self.intercept is None:
                            self.datas.append(odata)
                        else:
                            intercept_datas.append(odata)

            while intercept_datas:
                data = intercept_datas.popleft()
                if self.intercept is not None and not self.intercept.fill_get(data):
                    continue
                self.datas.append(data)
            self.geted = True
            return self.datas

        GeneratorType, GeneratorFunctionTypes, FunctionType = types.GeneratorType, (types.FunctionType, types.GeneratorType), types.FunctionType
        oyield_generates, oyields, ofuncs, getter_datas = deque(), {}, {}, deque()
        while datas:
            data, odata, predicate_yield = datas.pop(), {}, None
            if isinstance(data, ContextDataer):
                data.contexter.values = data.values
                if self.predicate is not None:
                    predicate_value = self.predicate.get()
                    if isinstance(predicate_value, GeneratorType):
                        predicate_yield = predicate_value
                    elif not predicate_value:
                        continue
                for name, valuer in self.schema.items():
                    value = valuer.get()
                    if isinstance(value, GeneratorFunctionTypes):
                        if isinstance(value, GeneratorType):
                            oyields[name] = value
                        else:
                            ofuncs[name] = value
                    else:
                        odata[name] = value
            else:
                if isinstance(data, tuple):
                    if isinstance(data[0], ContextRunner):
                        predicate_value = data[0].get()
                        if isinstance(predicate_value, GeneratorType):
                            predicate_yield = predicate_value
                        elif not predicate_value:
                            continue
                    elif self.predicate is not None and not self.predicate.fill_get(data[1]):
                        continue
                    data = data[1]
                else:
                    if self.predicate is not None and not self.predicate.fill_get(data):
                        continue
                for name, valuer in self.schema.items():
                    if name not in data or not isinstance(data[name], ContextRunner):
                        value = valuer.fill_get(data)
                    else:
                        value = data[name].get()
                    if isinstance(value, GeneratorFunctionTypes):
                        if isinstance(value, GeneratorType):
                            oyields[name] = value
                        else:
                            ofuncs[name] = value
                    else:
                        odata[name] = value

            if predicate_yield or oyields:
                while True:
                    predicate_continue = False
                    while oyields:
                        oyield_odata, oyield_oyields, oyield_ofuncs, predicate_oyield = dict.copy(odata), {}, dict.copy(ofuncs), None
                        if predicate_yield:
                            try:
                                predicate_value = predicate_yield.send(None)
                                if isinstance(predicate_value, GeneratorType):
                                    predicate_oyield, predicate_continue = predicate_value, False
                                else:
                                    predicate_continue = True if not predicate_value else False
                            except StopIteration:
                                predicate_yield, predicate_continue = None, True

                        has_oyield_data = False
                        for name, oyield in tuple(oyields.items()):
                            try:
                                value = oyield.send(None)
                                if isinstance(value, GeneratorFunctionTypes):
                                    if isinstance(value, GeneratorType):
                                        oyield_oyields[name] = value
                                    else:
                                        oyield_ofuncs[name] = value
                                else:
                                    oyield_odata[name] = value
                                has_oyield_data = True
                            except StopIteration:
                                oyields.pop(name)
                        if predicate_continue:
                            continue
                        if oyield_oyields:
                            oyield_generates.appendleft((odata, oyields, ofuncs, predicate_yield))
                            odata, oyields, ofuncs, predicate_yield = oyield_odata, oyield_oyields, oyield_ofuncs, predicate_oyield
                            continue

                        if has_oyield_data:
                            if oyield_ofuncs:
                                has_func_data, ogetter_funcs = True, {}
                                for name, ofunc in oyield_ofuncs.items():
                                    try:
                                        value = ofunc(oyield_odata)
                                        if isinstance(value, FunctionType):
                                            ogetter_funcs[name] = value
                                        else:
                                            oyield_odata[name] = value
                                    except StopIteration:
                                        has_func_data = False
                                        continue
                                if has_func_data:
                                    if ogetter_funcs:
                                        fgetter_funcs = {}
                                        for name, getter_func in ogetter_funcs.items():
                                            value = getter_func()
                                            if isinstance(value, FunctionType):
                                                fgetter_funcs[name] = value
                                            else:
                                                oyield_odata[name] = value
                                        getter_datas.append((oyield_odata, fgetter_funcs))
                                    else:
                                        getter_datas.append((oyield_odata, None))
                                oyield_ofuncs.clear()
                            else:
                                getter_datas.append((oyield_odata, None))

                    oyields.clear()
                    ofuncs.clear()
                    if not oyield_generates:
                        break
                    odata, oyields, ofuncs, predicate_yield = oyield_generates.popleft()
            else:
                if ofuncs:
                    has_func_data, ogetter_funcs = True, {}
                    for name, ofunc in ofuncs.items():
                        try:
                            value = ofunc(odata)
                            if isinstance(value, FunctionType):
                                ogetter_funcs[name] = value
                            else:
                                odata[name] = value
                        except StopIteration:
                            has_func_data = False
                            continue
                    if has_func_data:
                        if ogetter_funcs:
                            fgetter_funcs = {}
                            for name, getter_func in ogetter_funcs.items():
                                value = getter_func()
                                if isinstance(value, FunctionType):
                                    fgetter_funcs[name] = value
                                else:
                                    odata[name] = value
                            getter_datas.append((odata, fgetter_funcs))
                        else:
                            getter_datas.append((odata, None))
                    ofuncs.clear()
                else:
                    getter_datas.append((odata, None))

        while getter_datas:
            odata, getter_funcs = getter_datas.popleft()
            if getter_funcs:
                for name, getter_func in getter_funcs.items():
                    odata[name] = getter_func()
            if self.intercept is not None and not self.intercept.fill_get(odata):
                continue
            self.datas.append(odata)
        self.geted = True
        return self.datas

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