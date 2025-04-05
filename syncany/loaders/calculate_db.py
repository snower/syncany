# -*- coding: utf-8 -*-
# 2025/3/24
# create by: snower

import copy
from collections import defaultdict
from ..loaders import Loader
from ..loaders import DBLoader
from ..valuers.valuer import LoadAllFieldsException


class CalculaterDBLoader(DBLoader):
    def __init__(self, calculater, calculater_kwargs, *args, **kwargs):
        Loader.__init__(self,*args, **kwargs)

        self.calculater = calculater
        self.calculater_kwargs = calculater_kwargs or {}
        self.contexter = False
        self.last_data = None

    def clone(self):
        loader = self.__class__(self.calculater, self.calculater_kwargs, self.name, self.primary_keys, self.valuer_type)
        loader.contexter = self.contexter
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
        loader.orders = [order for order in self.orders]
        loader.predicates = [predicate.clone() for predicate in self.predicates]
        loader.intercepts = [intercept.clone() for intercept in self.intercepts]
        loader.key_matchers = [matcher.clone() for matcher in self.key_matchers]
        return loader

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
        if self.loaded:
            return

        fields = set([])
        if not self.key_matchers:
            try:
                for name, valuer in self.schema.items():
                    for field in valuer.get_fields():
                        fields.add(field)
            except LoadAllFieldsException:
                fields = []

        query = {"fields": fields, "filters": defaultdict(list), "orders": []}
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
                query["filters"][exp].append(value)
            else:
                query["filters"][exp].append((key, value))

        primary_orders = {} if len(self.orders) >= len(self.primary_keys) else None
        for i in range(len(self.orders)):
            order = self.orders[i]
            query["orders"].append(order)
            if primary_orders is None:
                continue
            if i < len(self.primary_keys) and order[0] != self.primary_keys[i]:
                primary_orders = None
                continue
            primary_orders[order[0]] = order[1]

        if self.current_cursor:
            query["cursor"] = (self.current_cursor, {"primary_orders": primary_orders})

        self.datas, query = self.calculater.calculate(self.primary_keys, query, **self.calculater_kwargs), None
        self.last_data = copy.copy(self.datas[-1]) if self.datas else {}
        self.loader_state["query_count"] += 1
        self.loader_state["load_count"] += len(self.datas)
        self.loaded = True