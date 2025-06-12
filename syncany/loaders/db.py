# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import copy
import types
from collections import defaultdict, deque
from .loader import Loader
from ..valuers.valuer import Contexter, ContextRunner, ContextDataer, LoadAllFieldsException


class DBLoader(Loader):
    def __init__(self, db, name, *args, **kwargs):
        super(DBLoader, self).__init__(*args, **kwargs)

        self.db = db
        self.name = name
        self.contexter = False
        self.last_data = None

    def config(self, tasker):
        super(DBLoader, self).config(tasker)
        self.db.config_loader(self)

    def clone(self):
        loader = self.__class__(self.db, self.name, self.primary_keys, self.valuer_type)
        loader.contexter = self.contexter
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

    def next(self):
        if not self.loaded:
            return True
        if self.db:
            return self.db.is_streaming(self.name)
        return False

    def is_dynamic_schema(self):
        return self.db.is_dynamic_schema(self.name)

    def is_streaming(self):
        return self.db.is_streaming(self.name)

    def set_streaming(self, is_streaming=None):
        if is_streaming is None:
            return
        self.db.set_streaming(self.name, is_streaming)

    def load(self, timeout=None):
        if self.loaded:
            return

        fields = set([])
        if self.predicate is not None:
            for field in self.predicate.get_fields():
                fields.add(field)
        if not self.key_matchers:
            try:
                for name, valuer in self.schema.items():
                    for field in valuer.get_fields():
                        fields.add(field)
            except LoadAllFieldsException:
                fields = []

        query = self.db.query(self.name, self.primary_keys, list(fields))

        in_exps = defaultdict(list)
        for key, exp, value in self.filters:
            if exp == "in":
                in_exps[key].extend(value)

        for key, exp, value in self.filters:
            if exp == "eq":
                in_exps[key].append(value)

        for key, exp, value in self.filters:
            if exp == "eq":
                if key not in in_exps:
                    continue

                if len(in_exps[key]) > 1:
                    exp, value = "in", in_exps.pop(key)

            if key is None:
                getattr(query, "filter_%s" % exp)(value)
            else:
                getattr(query, "filter_%s" % exp)(key, value)

        primary_orders = {} if len(self.orders) >= len(self.primary_keys) else None
        for i in range(len(self.orders)):
            order = self.orders[i]
            query.order_by(*order)
            if primary_orders is None:
                continue
            if i < len(self.primary_keys) and order[0] != self.primary_keys[i]:
                primary_orders = None
                continue
            primary_orders[order[0]] = order[1]

        if self.current_cursor:
            query.filter_cursor(*self.current_cursor, primary_orders=primary_orders)

        self.datas, query = query.commit(), None
        self.last_data = copy.copy(self.datas[-1]) if self.datas else {}
        self.loader_state["query_count"] += 1
        self.loader_state["load_count"] += len(self.datas)
        self.loaded = True

    def get(self):
        if self.geted:
            return self.datas
        if not self.loaded:
            self.load()

        if not self.key_matchers:
            loader_contexter = self.contexter
            if loader_contexter is False:
                if not self.valuer_type:
                    return self.fast_get()
                if self.valuer_type == 0x02:
                    return self.fast_aggregate_get()
                if self.valuer_type in (0x04, 0x06):
                    return self.fast_partition_aggregate_get()
                return super(DBLoader, self).get()

            if loader_contexter is not None:
                for i in range(len(self.datas)):
                    data, context_dataer = self.datas[i], ContextDataer(loader_contexter)
                    loader_contexter.values = context_dataer.values
                    if self.predicate is not None:
                        self.predicate.fill(data)
                    for key, field in self.schema.items():
                        field.fill(data)
                    self.datas[i] = context_dataer
                if self.valuer_type == 0x03:
                    return self.fast_join_aggregate_get()
                if self.valuer_type == 0x01:
                    return self.fast_join_get()
                return super(DBLoader, self).get()

            contexter_schema = [(key, field, field.contexter if hasattr(field, "contexter") else None)
                                for key, field in self.schema.items()]
            for i in range(len(self.datas)):
                data, predicate, odata, contexter_values = self.datas[i], None, {}, {}
                if self.predicate is not None:
                    if hasattr(self.predicate, "contexter"):
                        contexter = self.predicate.contexter
                        predicate = self.predicate
                    else:
                        contexter = Contexter()
                        predicate = self.predicate.clone(contexter)
                    predicate = ContextRunner(contexter, predicate, contexter_values).fill(data)
                for key, field, contexter in contexter_schema:
                    if contexter is None:
                        contexter = Contexter()
                        field = field.clone(contexter)
                    odata[key] = ContextRunner(contexter, field, contexter_values).fill(data)
                self.datas[i] = (predicate, odata)
            return super(DBLoader, self).get()

        for i in range(len(self.datas)):
            data = {}
            for key, value in self.datas[i].items():
                if key in self.schema:
                    data[key] = value
                    continue
                for key_matcher in self.key_matchers:
                    if not key_matcher.match(key):
                        continue
                    self.schema[key] = key_matcher.create_key(key)
                    data[key] = value
                    break
            self.datas[i] = data
        return super(DBLoader, self).get()

    def fast_get(self):
        if self.predicate is None and self.intercept is None:
            for i in range(len(self.datas)):
                data = self.datas[i]
                self.datas[i] = {name: valuer.fill_get(data) for name, valuer in self.schema.items()}
            self.geted = True
            return self.datas

        datas, self.datas = self.datas, []
        datas.reverse()
        while datas:
            data = datas.pop()
            if self.predicate is not None and not self.predicate.fill_get(data):
                continue
            odata = {name: valuer.fill_get(data) for name, valuer in self.schema.items()}
            if self.intercept is not None and not self.intercept.fill_get(odata):
                continue
            self.datas.append(odata)
        self.geted = True
        return self.datas

    def fast_join_get(self):
        datas, self.datas = self.datas, []
        datas.reverse()
        GeneratorType = types.GeneratorType

        oyield_generates, oyields = deque(), {}
        if self.predicate is None and self.intercept is None:
            while datas:
                data, odata, = datas.pop(), {}
                data.contexter.values = data.values
                for name, valuer in self.schema.items():
                    value = valuer.get()
                    if isinstance(value, GeneratorType):
                        oyields[name] = value
                        odata[name] = None
                    else:
                        odata[name] = value

                if oyields:
                    while True:
                        while oyields:
                            oyield_odata, oyield_oyields = dict.copy(odata), {}
                            has_oyield_data = False
                            for name, oyield in tuple(oyields.items()):
                                try:
                                    value = oyield.send(None)
                                    if isinstance(value, GeneratorType):
                                        oyield_oyields[name] = value
                                    else:
                                        oyield_odata[name] = value
                                    has_oyield_data = True
                                except StopIteration:
                                    oyields.pop(name)
                            if oyield_oyields:
                                oyield_generates.appendleft((odata, oyields))
                                odata, oyields = oyield_odata, oyield_oyields
                                continue
                            if has_oyield_data:
                                self.datas.append(oyield_odata)
                        if not oyield_generates:
                            break
                        odata, oyields = oyield_generates.popleft()
                else:
                    self.datas.append(odata)
            self.geted = True
            return self.datas

        while datas:
            data, odata, predicate_yield = datas.pop(), {}, None
            data.contexter.values = data.values
            if self.predicate is not None:
                predicate_value = self.predicate.get()
                if isinstance(predicate_value, GeneratorType):
                    predicate_yield = predicate_value
                elif not predicate_value:
                    continue
            for name, valuer in self.schema.items():
                value = valuer.get()
                if isinstance(value, GeneratorType):
                    oyields[name] = value
                    odata[name] = None
                else:
                    odata[name] = value

            if predicate_yield or oyields:
                while True:
                    predicate_continue = False
                    while predicate_yield or oyields:
                        oyield_odata, oyield_oyields, predicate_oyield = dict.copy(odata), {}, None
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
                                if isinstance(value, GeneratorType):
                                    oyield_oyields[name] = value
                                else:
                                    oyield_odata[name] = value
                                has_oyield_data = True
                            except StopIteration:
                                oyields.pop(name)
                        if predicate_continue:
                            continue
                        if oyield_oyields or predicate_oyield:
                            oyield_generates.appendleft((odata, oyields, predicate_yield))
                            odata, oyields, predicate_yield = oyield_odata, oyield_oyields, predicate_oyield
                            continue
                        if has_oyield_data:
                            if self.intercept is not None and not self.intercept.fill_get(oyield_odata):
                                continue
                            self.datas.append(oyield_odata)
                    if not oyield_generates:
                        break
                    odata, oyields, predicate_yield = oyield_generates.popleft()
            else:
                if self.intercept is not None and not self.intercept.fill_get(odata):
                    continue
                self.datas.append(odata)
        self.geted = True
        return self.datas

    def fast_aggregate_get(self):
        datas, self.datas = self.datas, []
        datas.reverse()
        FunctionType, ofuncs = types.FunctionType, {}

        if self.predicate is None and self.intercept is None:
            while datas:
                data, odata, = datas.pop(), {}
                for name, valuer in self.schema.items():
                    value = valuer.fill_get(data)
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
                        self.datas.append(odata)
                    ofuncs.clear()
                else:
                    self.datas.append(odata)
            self.geted = True
            return self.datas

        intercept_datas = deque() if self.intercept is not None else None
        while datas:
            data, odata, = datas.pop(), {}
            if self.predicate is not None and not self.predicate.fill_get(data):
                continue
            for name, valuer in self.schema.items():
                value = valuer.fill_get(data)
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

    def fast_join_aggregate_get(self):
        datas, self.datas = self.datas, []
        datas.reverse()

        GeneratorType, GeneratorFunctionTypes = types.GeneratorType, (types.FunctionType, types.GeneratorType)
        oyield_generates, oyields, ofuncs = deque(), {}, {}
        if self.predicate is None and self.intercept is None:
            while datas:
                data, odata = datas.pop(), {}
                data.contexter.values = data.values
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

                if oyields:
                    while True:
                        while oyields:
                            oyield_odata, oyield_oyields, oyield_ofuncs = dict.copy(odata), {}, dict.copy(ofuncs)
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
                            if oyield_oyields:
                                oyield_generates.appendleft((odata, oyields, ofuncs))
                                odata, oyields, ofuncs = oyield_odata, oyield_oyields, oyield_ofuncs
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
                                        self.datas.append(oyield_odata)
                                    oyield_ofuncs.clear()
                                else:
                                    self.datas.append(oyield_odata)

                        oyields.clear()
                        ofuncs.clear()
                        if not oyield_generates:
                            break
                        odata, oyields, ofuncs = oyield_generates.popleft()
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
                            self.datas.append(odata)
                        ofuncs.clear()
                    else:
                        self.datas.append(odata)
            self.geted = True
            return self.datas

        intercept_datas = deque() if self.intercept is not None else None
        while datas:
            data, odata, predicate_yield = datas.pop(), {}, None
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

            if predicate_yield or oyields:
                while True:
                    predicate_continue = False
                    while oyields:
                        oyield_odata, oyield_oyields, oyield_ofuncs, predicate_oyield = dict.copy(odata), {}, dict.copy(
                            ofuncs), None
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

    def fast_partition_aggregate_get(self):
        datas, self.datas = self.datas, []
        datas.reverse()
        FunctionType, ofuncs, getter_datas = types.FunctionType, {}, deque()

        if self.predicate is None and self.intercept is None:
            while datas:
                data, odata, = datas.pop(), {}
                for name, valuer in self.schema.items():
                    value = valuer.fill_get(data)
                    if isinstance(value, FunctionType):
                        ofuncs[name] = value
                        odata[name] = None
                    else:
                        odata[name] = value

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
                self.datas.append(odata)
            self.geted = True
            return self.datas

        while datas:
            data, odata, = datas.pop(), {}
            if self.predicate is not None and not self.predicate.fill_get(data):
                continue
            for name, valuer in self.schema.items():
                value = valuer.fill_get(data)
                if isinstance(value, FunctionType):
                    ofuncs[name] = value
                    odata[name] = None
                else:
                    odata[name] = value

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

    def statistics(self):
        return {
            "querys": self.loader_state["query_count"],
            "rows": self.loader_state["load_count"]
        }