# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from collections import defaultdict
from .db import DBLoader
from ..valuers.valuer import LoadAllFieldsException


class DBJoinMatcher(object):
    valuer = None
    contexter_values = None

    def fill(self, values):
        if self.contexter_values is not None:
            self.valuer.contexter.values = self.contexter_values
        self.valuer.fill(values)

    def add_valuer(self, valuer):
        self.valuer = valuer
        self.contexter_values = valuer.contexter.values if hasattr(valuer, "contexter") else None

    def get(self):
        if self.contexter_values is not None:
            self.valuer.contexter.values = self.contexter_values
        return self.valuer.get()


class GroupDBJoinMatcher(object):
    def __init__(self, is_aggregate, is_yield):
        self.is_aggregate = is_aggregate
        self.is_yield = is_yield
        self.matchers = []

    def add_matcher(self, matcher):
        self.matchers.append(matcher)

    def get(self):
        if not self.is_yield and not self.is_aggregate:
            return [matcher.get() for matcher in self.matchers]

        def gen_iter(iter_datas):
            yield None
            for value in iter_datas:
                if value is None:
                    continue
                yield value
        g = gen_iter([matcher.get() for matcher in self.matchers])
        g.send(None)
        return g


class DBJoinLoader(DBLoader):
    def __init__(self, *args, **kwargs):
        self.join_batch = kwargs.pop("join_batch", 10000) or 0xffffffff
        super(DBJoinLoader, self).__init__(*args, **kwargs)

        self.data_keys = {}
        self.unload_primary_keys = set([])
        self.load_primary_keys = set([])
        self.matchers = defaultdict(list)

    def clone(self):
        loader = super(DBJoinLoader, self).clone()
        loader.join_batch = self.join_batch
        if len(self.data_keys) < self.join_batch:
            loader.data_keys = self.data_keys
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

    def create_group_macther(self, is_aggregate, is_yield):
        return GroupDBJoinMatcher(is_aggregate, is_yield)

    def create_macther(self, keys, values):
        matcher = DBJoinMatcher()
        if len(self.primary_keys) == 1:
            self.matchers[values[0]].append(matcher)
            if values[0] not in self.data_keys:
                self.unload_primary_keys.add(values[0])
            else:
                self.load_primary_keys.add(values[0])
        else:
            data_key = tuple(values)
            self.matchers[data_key].append(matcher)
            if data_key not in self.data_keys:
                self.unload_primary_keys.add(data_key)
            else:
                self.load_primary_keys.add(data_key)

        self.loaded = False
        return matcher

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

            query = self.db.query(self.name, self.primary_keys, list(fields))
            for key, exp, value in self.filters:
                if key is None:
                    getattr(query, "filter_%s" % exp)(value)
                else:
                    getattr(query, "filter_%s" % exp)(key, value)

            if len(self.primary_keys) == 1:
                query.filter_in(self.primary_keys[0], list(self.unload_primary_keys))
            else:
                for j in range(len(self.primary_keys)):
                    query.filter_in(self.primary_keys[j], list({primary_value[j] for primary_value
                                                                in self.unload_primary_keys}))
            datas, query = query.commit(), None

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

    def try_load(self):
        if self.loaded:
            return
        if len(self.unload_primary_keys) >= self.join_batch:
            return self.load_join()
        if not self.load_primary_keys:
            return

        for primary_key in self.load_primary_keys:
            matchers = self.matchers.pop(primary_key, None)
            if not matchers:
                continue
            values = self.data_keys.get(primary_key, None)
            for matcher in matchers:
                matcher.fill(values)

        self.load_primary_keys.clear()
        if not self.unload_primary_keys:
            self.loaded = True

    def load(self, timeout=None):
        self.load_join()
        if len(self.data_keys) >= self.join_batch:
            self.datas, self.data_keys = [], {}
