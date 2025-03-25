# -*- coding: utf-8 -*-
# 2025/3/24
# create by: snower

from collections import defaultdict
from ..loaders import Loader
from ..loaders import DBJoinLoader
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

    def load_join(self):
        if self.loaded:
            return

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

            if len(self.primary_keys) == 1:
                query["filters"]["in"].append((self.primary_keys[0], list(self.unload_primary_keys)))
            else:
                for j in range(len(self.primary_keys)):
                    query["filters"]["in"].append((self.primary_keys[j], list({primary_value[j] for primary_value
                                                                          in self.unload_primary_keys})))
            datas, query = self.calculater.calculate(self.primary_keys, query, **self.calculater_kwargs), None

            if not self.key_matchers:
                for i in range(len(datas)):
                    data = datas[i]
                    primary_key = self.get_data_primary_key(data)
                    values = {key: field.fill_get(data) for key, field in self.schema.items()}

                    if primary_key not in self.data_keys:
                        self.data_keys[primary_key] = values
                    elif primary_key in self.unload_primary_keys:
                        if isinstance(self.data_keys[primary_key], list):
                            self.data_keys[primary_key].append(values)
                        else:
                            self.data_keys[primary_key] = [self.data_keys[primary_key], values]
                    datas[i] = values
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
                        if isinstance(self.data_keys[primary_key], list):
                            self.data_keys[primary_key].append(values)
                        else:
                            self.data_keys[primary_key] = [self.data_keys[primary_key], values]
                    datas[i] = values

            self.datas = datas
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