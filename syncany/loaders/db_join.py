# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from collections import defaultdict
from .db import DBLoader
from ..valuers.case import CaseValuer

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
        if isinstance(values, (list, tuple, set)):
            self.data = [{key: valuer.get() for key, valuer in value.items()} for value in values]
        else:
            self.data = {key: valuer.get() for key, valuer in values.items()}

        for valuer in self.valuers:
            valuer.fill(self.data)

    def add_valuer(self, valuer):
        self.valuers.append(valuer)

    def get(self):
        return self.data

class DBJoinLoader(DBLoader):
    def __init__(self, *args, **kwargs):
        super(DBJoinLoader, self).__init__(*args, **kwargs)

        self.unload_primary_keys = set([])
        self.matchers = defaultdict(list)

    def filter_eq(self, key, value):
        if key != self.primary_keys[0]:
            self.primary_keys = [key]

        matcher = DBJoinMatcher(key, value)
        self.matchers[value].append(matcher)

        if value not in self.data_keys:
            self.unload_primary_keys.add(value)

        self.loaded = False
        return matcher

    def load(self):
        if self.loaded:
            return

        if self.unload_primary_keys:
            fields = set([])
            if not self.key_matchers:
                for key, exp, value in self.filters:
                    if key: fields.add(key)

                for name, valuer in self.schema.items():
                    for field in valuer.get_fields():
                        fields.add(field)

            unload_primary_keys = list(self.unload_primary_keys)
            for i in range(int(len(unload_primary_keys) / 1000.0 + 1)):
                query = self.db.query(self.name, self.primary_keys, list(fields))
                for key, exp, value in self.filters:
                    if key is None:
                        getattr(query, "filter_%s" % exp)(value)
                    else:
                        getattr(query, "filter_%s" % exp)(key, value)

                query.filter_in(self.primary_keys[0], unload_primary_keys[i * 1000: (i + 1) * 1000])
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

                self.querys.append(query)

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

        self.loaded = True
