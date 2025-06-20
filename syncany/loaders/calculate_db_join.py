# -*- coding: utf-8 -*-
# 2025/3/24
# create by: snower

import math
from collections import defaultdict
from ..loaders import Loader, DBJoinLoader
from ..valuers.valuer import LoadAllFieldsException


class CalculaterDBJoinLoader(DBJoinLoader):
    def __init__(self, calculater, calculater_kwargs, *args, **kwargs):
        self.join_batch = kwargs.pop("join_batch", 10000) or 0xffffffff
        Loader.__init__(self, *args, **kwargs)

        self.calculater = calculater
        self.calculater_kwargs = calculater_kwargs or {}
        self.contexter = False
        self.last_data = None
        self.data_keys = {}
        self.unload_primary_keys = set([])
        self.load_primary_keys = set([])
        self.matchers = defaultdict(list)

    def config(self, tasker):
        Loader.config(self, tasker)

    def clone(self):
        loader = self.__class__(self.calculater, self.calculater_kwargs, self.primary_keys, self.valuer_type)
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
        loader.join_batch = self.join_batch
        if len(self.data_keys) < self.join_batch:
            loader.data_keys = self.data_keys
        return loader

    def load_join(self):
        if self.unload_primary_keys:
            fields = set([])
            if not self.key_matchers:
                for key, exp, value in self.filters:
                    if not key:
                        continue
                    fields.add(key)
                try:
                    for name, valuer in self.schema.items():
                        for field in valuer.get_fields():
                            fields.add(field)
                except LoadAllFieldsException:
                    fields = []

            query = {"fields": fields, "filters": defaultdict(list), "orders": []}
            for key, exp, value in self.filters:
                if key is None:
                    query["filters"][exp].append(value)
                else:
                    query["filters"][exp].append((key, value))

            if len(self.unload_primary_keys) <= self.join_batch:
                if len(self.primary_keys) == 1:
                    query["filters"]["in"].append((self.primary_keys[0], list(self.unload_primary_keys)))
                else:
                    for j in range(len(self.primary_keys)):
                        query["filters"]["in"].append((self.primary_keys[j], list({primary_value[j] for primary_value
                                                                                   in self.unload_primary_keys})))
                datas, query = self.calculater.calculate(self.primary_keys, query, **self.calculater_kwargs), None
            else:
                unload_primary_keys, datas = list(self.unload_primary_keys), []
                for i in range(math.ceil(float(len(unload_primary_keys)) / float(self.join_batch))):
                    current_unload_primary_keys = unload_primary_keys[i * self.join_batch: (i + 1) * self.join_batch]
                    if not current_unload_primary_keys:
                        break
                    if len(self.primary_keys) == 1:
                        query["filters"]["in"].append((self.primary_keys[0], current_unload_primary_keys))
                    else:
                        for j in range(len(self.primary_keys)):
                            query["filters"]["in"].append((self.primary_keys[j], list({primary_value[j] for primary_value
                                                                                       in current_unload_primary_keys})))
                    query = datas.extend(self.calculater.calculate(self.primary_keys, query, **self.calculater_kwargs))

            if not self.key_matchers:
                if len(self.primary_keys) == 1:
                    for data in datas:
                        primary_key = data.get(self.primary_keys[0], '')
                        if primary_key not in self.data_keys:
                            self.data_keys[primary_key] = data
                        elif primary_key in self.unload_primary_keys:
                            if self.data_keys[primary_key].__class__ is list:
                                self.data_keys[primary_key].append(data)
                            else:
                                self.data_keys[primary_key] = [self.data_keys[primary_key], data]
                else:
                    for data in datas:
                        primary_key = tuple(data.get(pk, '') for pk in self.primary_keys)
                        if primary_key not in self.data_keys:
                            self.data_keys[primary_key] = data
                        elif primary_key in self.unload_primary_keys:
                            if self.data_keys[primary_key].__class__ is list:
                                self.data_keys[primary_key].append(data)
                            else:
                                self.data_keys[primary_key] = [self.data_keys[primary_key], data]
            else:
                for i in range(len(datas)):
                    data, values = datas[i], {}
                    primary_key = self.get_data_primary_key(data)
                    for key, value in data.items():
                        if key in self.schema:
                            values[key] = self.schema[key].fill_get(data)
                        else:
                            for key_matcher in self.key_matchers:
                                if key_matcher.match(key):
                                    valuer = key_matcher.create_key(key)
                                    self.schema[key] = valuer
                                    values[key] = valuer.fill_get(data)

                    if primary_key not in self.data_keys:
                        self.data_keys[primary_key] = values
                    elif primary_key in self.unload_primary_keys:
                        if self.data_keys[primary_key].__class__ is list:
                            self.data_keys[primary_key].append(values)
                        else:
                            self.data_keys[primary_key] = [self.data_keys[primary_key], values]
                    datas[i] = values

            self.loader_state["query_count"] += 1
            self.loader_state["load_count"] += len(datas)

        if self.matchers:
            for primary_key in self.load_primary_keys:
                matchers = self.matchers.pop(primary_key, None)
                if not matchers:
                    continue
                values = self.data_keys.get(primary_key, None)
                for matcher in matchers:
                    matcher.fill(values)

            for primary_key in self.unload_primary_keys:
                matchers = self.matchers.pop(primary_key, None)
                if not matchers:
                    continue
                values = self.data_keys.get(primary_key, None)
                for matcher in matchers:
                    matcher.fill(values)

        self.unload_primary_keys = set([])
        self.load_primary_keys = set([])
        self.loaded = True