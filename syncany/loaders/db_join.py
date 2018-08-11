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

    def fill(self, values):
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
            for key, exp, value in self.filters:
                fields.add(key)

            for name, valuer in self.schema.items():
                for field in valuer.get_fields():
                    fields.add(field)
            query = self.db.query(self.name, *list(fields))

            for key, exp, value in self.filters:
                getattr(query, "filter_%s" % exp)(key, value)

            query.filter_in(self.primary_keys[0], list(self.unload_primary_keys))

            datas = query.commit()
            for data in datas:
                primary_key = self.get_data_primary_key(data)

                values = {}
                for key, field in self.schema.items():
                    values[key] = field.clone().fill(data)

                self.data_keys[primary_key] = values
                self.datas.append(values)

            self.unload_primary_keys = set([])
            self.querys.append(query)

        if self.matchers:
            for data in self.datas:
                value = data[self.primary_keys[0]].get()
                if value in self.matchers:
                    for matcher in self.matchers[value]:
                        matcher.fill(data)
                    self.matchers.pop(value)

        self.loaded = True
