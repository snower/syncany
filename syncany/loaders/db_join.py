# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from collections import defaultdict
from .db import DBLoader
from ..valuers.valuer import LoadAllFieldsException


class DBJoinYieldMatcher(object):
    intercept_valuer = None
    valuer = None
    contexter_values = None
    data_valuers = None

    def __init__(self, intercept_valuer, valuer, contexter_values):
        if intercept_valuer is not None:
            self.intercept_valuer = intercept_valuer
        self.valuer = valuer
        if contexter_values is not None:
            self.contexter_values = contexter_values

    def fill(self, values):
        if self.intercept_valuer:
            if isinstance(values, list):
                ovalues = []
                if self.intercept_valuer.intercept_wait_loaded:
                    if self.contexter_values is not None:
                        intercept_contexter_valueses = []
                        for value in values:
                            self.intercept_valuer.contexter.values = \
                                self.intercept_valuer.contexter.create_inherit_values(self.contexter_values)
                            self.intercept_valuer.fill(value)
                            intercept_contexter_valueses.append((value, self.intercept_valuer.contexter.values))
                        for value, intercept_contexter_values in intercept_contexter_valueses:
                            self.intercept_valuer.contexter.values = intercept_contexter_values
                            if not self.intercept_valuer.get():
                                continue
                            ovalues.append(value)
                        self.intercept_valuer.contexter.values = self.contexter_values
                    else:
                        intercept_valuers = []
                        for value in values:
                            intercept_valuers.append((value, self.intercept_valuer.clone(inherited=True).fill(value)))
                        for value, intercept_valuer in intercept_valuers:
                            if not intercept_valuer.get():
                                continue
                            ovalues.append(value)
                else:
                    if self.contexter_values is not None:
                        self.intercept_valuer.contexter.values = self.contexter_values
                    for value in values:
                        if not self.intercept_valuer.fill_get(value):
                            continue
                        ovalues.append(value)
                values = ovalues if len(ovalues) > 1 else (ovalues[0] if ovalues else None)
            elif values is not None:
                if self.contexter_values is not None:
                    self.intercept_valuer.contexter.values = self.contexter_values
                if not self.intercept_valuer.fill_get(values):
                    values = None

        if isinstance(values, list):
            self.data_valuers = []
            if self.contexter_values is not None:
                for value in values:
                    self.valuer.contexter.values = self.valuer.contexter.create_inherit_values(self.contexter_values)
                    self.valuer.fill(value)
                    self.data_valuers.append((self.valuer, self.valuer.contexter.values))
            else:
                for value in values:
                    valuer = self.valuer.clone(inherited=True).fill(value)
                    self.data_valuers.append((valuer, None))
        else:
            if self.contexter_values is not None:
                self.valuer.contexter.values = self.contexter_values
            self.valuer.fill(values)
            if values is None:
                self.data_valuers = False

    def get(self, is_in_depth_citation=True):
        if not self.data_valuers:
            if self.contexter_values is not None:
                self.valuer.contexter.values = self.contexter_values
            if self.data_valuers is None:
                return self.valuer.get()
            value = self.valuer.get()
            if value is not None:
                return value
            values = []
        else:
            values = []
            for valuer, contexter_values in self.data_valuers:
                if contexter_values is not None:
                    valuer.contexter.values = contexter_values
                values.append(valuer.get())
            if len(values) == 1:
                return values[0]
        if not is_in_depth_citation:
            return values or None

        def gen_iter(iter_datas):
            for value in iter_datas:
                yield value
        return gen_iter(values)


class DBJoinMatcher(object):
    intercept_valuer = None
    valuer = None
    group_matcher = None
    contexter_values = None

    def __init__(self, intercept_valuer, valuer, contexter_values):
        if intercept_valuer is not None:
            self.intercept_valuer = intercept_valuer
        if valuer is not None:
            self.valuer = valuer
        if contexter_values is not None:
            self.contexter_values = contexter_values

    def fill(self, values):
        if self.intercept_valuer:
            if self.contexter_values is not None:
                self.intercept_valuer.contexter.values = self.contexter_values

            if isinstance(values, list):
                ovalues = []
                for value in values:
                    if not self.intercept_valuer.fill_get(value):
                        continue
                    ovalues.append(value)
                values = ovalues if len(ovalues) > 1 else (ovalues[0] if ovalues else None)
            elif values is not None:
                if not self.intercept_valuer.fill_get(values):
                    values = None

        if self.valuer:
            if self.contexter_values is not None:
                self.valuer.contexter.values = self.contexter_values
            self.valuer.fill(values)
        elif self.group_matcher:
            self.group_matcher.fill(values)

    def get(self, is_in_depth_citation=True):
        if not self.valuer:
            return None
        if self.contexter_values is not None:
            self.valuer.contexter.values = self.contexter_values
        return self.valuer.get()


class GroupDBJoinYieldMatcher(object):
    def __init__(self):
        self.matchers = []

    def add_matcher(self, matcher):
        self.matchers.append(matcher)

    def get(self, is_in_depth_citation=True):
        datas = [matcher.get() for matcher in self.matchers]
        if len(datas) == 1:
            return datas[0]
        if not is_in_depth_citation:
            return datas or None

        def gen_iter(iter_datas):
            for value in iter_datas:
                yield value
        return gen_iter(datas)


class GroupDBJoinMatcher(object):
    def __init__(self, return_valuer, contexter_values):
        self.return_valuer = return_valuer
        self.contexter_values = contexter_values
        self.matcher_count = 0
        self.values = []

    def add_matcher(self, matcher):
        matcher.group_matcher = self
        self.matcher_count += 1

    def fill(self, values):
        self.values.append(values)
        if len(self.values) >= self.matcher_count:
            values = []
            for value in self.values:
                if isinstance(value, list):
                    values.extend(value)
                else:
                    values.append(value)
            if self.contexter_values is not None:
                self.return_valuer.contexter.values = self.contexter_values
            self.return_valuer.fill(values)

    def get(self, is_in_depth_citation=True):
        if self.contexter_values is not None:
            self.return_valuer.contexter.values = self.contexter_values
        if len(self.values) < self.matcher_count:
            values = []
            for value in self.values:
                if isinstance(value, list):
                    values.extend(value)
                else:
                    values.extend(value)
            self.return_valuer.fill(values)
        return self.return_valuer.get()


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

    def create_group_matcher(self, is_yield=False, **kwargs):
        if is_yield:
            return GroupDBJoinYieldMatcher()
        return GroupDBJoinMatcher(**kwargs)

    def create_matcher(self, keys, values, is_yield=False, **kwargs):
        matcher = DBJoinYieldMatcher(**kwargs) if is_yield else DBJoinMatcher(**kwargs)
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
