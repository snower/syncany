# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import math
from collections import defaultdict
from .db import DBLoader
from ..valuers.valuer import LoadAllFieldsException

class DBJoinMatcher(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.data = None
        self.valuers = []

    def clone(self):
        matcher = self.__class__(self.key, self.value)
        return matcher

    def fill(self, values):
        if values is None:
            self.data = None
        elif isinstance(values, list):
            self.data = [{key: valuer.get() for key, valuer in value.items()} for value in values]
        else:
            self.data = {key: valuer.get() for key, valuer in values.items()}

        for valuer in self.valuers:
            valuer.fill(self.data)

    def add_valuer(self, valuer):
        self.valuers.append(valuer)

    def get(self):
        return self.data

class GroupDBJoinMatcher(object):
    def __init__(self, return_valuer):
        self.return_valuer = return_valuer
        self.valuers = []
        self.datas = None

    def add_valuer(self, valuer):
        self.valuers.append(valuer)

    def fill(self, valuer, data):
        if self.datas is not None:
            return self
        for valuer in self.valuers:
            if valuer.loaded is False:
                return self

        self.datas = []
        for valuer in self.valuers:
            if valuer.loaded is not True:
                continue
            value = valuer.get()
            if isinstance(value, list):
                self.datas.extend(value)
            else:
                self.datas.append(value)
        self.return_valuer.fill(self.datas)

    def get(self):
        if self.datas is not None:
            return self.datas

        self.datas = []
        for valuer in self.valuers:
            if valuer.loaded is not True:
                continue
            value = valuer.get()
            if isinstance(value, list):
                self.datas.extend(value)
            else:
                self.datas.append(value)
        self.return_valuer.fill(self.datas)
        return self.datas

class DBJoinLoader(DBLoader):
    def __init__(self, *args, **kwargs):
        self.join_batch = kwargs.pop("join_batch", 10000) or 0xffffffff
        super(DBJoinLoader, self).__init__(*args, **kwargs)

        self.data_keys = {}
        self.unload_primary_keys = set([])
        self.matchers = defaultdict(list)

    def clone(self):
        loader = super(DBJoinLoader, self).clone()
        loader.join_batch = self.join_batch
        return loader

    def create_group_macther(self, return_valuer):
        return GroupDBJoinMatcher(return_valuer)

    def filter_eq(self, key, value):
        if key != self.primary_keys[0]:
            self.primary_keys = [key]

        matcher = DBJoinMatcher(key, value)
        self.matchers[value].append(matcher)

        if value not in self.data_keys:
            self.unload_primary_keys.add(value)

        self.loaded = False
        return matcher

    def load(self, timeout=None):
        if self.loaded:
            return

        unload_primary_keys = None
        if self.unload_primary_keys:
            fields = set([])
            if not self.key_matchers:
                for key, exp, value in self.filters:
                    if key:
                        fields.add(key)

                try:
                    for name, valuer in self.schema.items():
                        for field in valuer.get_fields():
                            fields.add(field)
                except LoadAllFieldsException:
                    fields = []

            unload_primary_keys = list(self.unload_primary_keys)
            for i in range(math.ceil(float(len(unload_primary_keys)) / float(self.join_batch))):
                current_unload_primary_keys = unload_primary_keys[i * self.join_batch: (i + 1) * self.join_batch]
                if not current_unload_primary_keys:
                    break

                query = self.db.query(self.name, self.primary_keys, list(fields))
                for key, exp, value in self.filters:
                    if key is None:
                        getattr(query, "filter_%s" % exp)(value)
                    else:
                        getattr(query, "filter_%s" % exp)(key, value)

                query.filter_in(self.primary_keys[0], current_unload_primary_keys)
                datas = query.commit()
                for data in datas:
                    primary_key = self.get_data_primary_key(data)

                    values = {}
                    if not self.key_matchers:
                        for key, field in self.schema.items():
                            values[key] = field.clone().fill(data)
                    else:
                        for key, value in data.items():
                            if key in self.schema:
                                values[key] = self.schema[key].clone().fill(data)
                            else:
                                for key_matcher in self.key_matchers:
                                    if key_matcher.match(key):
                                        valuer = key_matcher.clone_valuer()
                                        valuer.key = key
                                        self.schema[key] = valuer
                                        values[key] = valuer.clone().fill(data)

                    if primary_key not in self.data_keys:
                        self.data_keys[primary_key] = [values]
                    else:
                        self.data_keys[primary_key].append(values)
                    self.datas.append(values)

                self.loader_state["query_count"] += 1
                self.loader_state["load_count"] += len(datas)

            self.unload_primary_keys = set([])

        if self.matchers:
            for primary_key, values in self.data_keys.items():
                if primary_key in self.matchers:
                    if len(values) == 1:
                        for matcher in self.matchers[primary_key]:
                            matcher.fill(values[0])
                    else:
                        for matcher in self.matchers[primary_key]:
                            matcher.fill(values)
                    self.matchers.pop(primary_key)
                if unload_primary_keys and primary_key in unload_primary_keys:
                    unload_primary_keys.remove(primary_key)
            if unload_primary_keys:
                for primary_key in unload_primary_keys:
                    if primary_key in self.matchers:
                        for matcher in self.matchers[primary_key]:
                            matcher.fill(None)
                        self.matchers.pop(primary_key)

        self.loaded = True
