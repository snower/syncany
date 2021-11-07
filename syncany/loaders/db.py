# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from collections import defaultdict
from .loader import Loader
from ..valuers.valuer import LoadAllFieldsException

class DBLoader(Loader):
    def __init__(self, db, name, *args, **kwargs):
        super(DBLoader, self).__init__(*args, **kwargs)

        self.db = db
        self.name = name
        self.compiled = False
        self.last_data = None

    def clone(self):
        loader = self.__class__(self.db, self.name, self.primary_keys, self.is_yield)
        schema = {}
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
        loader.orders = [order for order in self.orders]
        loader.key_matchers = [matcher.clone() for matcher in self.key_matchers]
        return loader

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

        if self.current_cursor:
            query.filter_cursor(*self.current_cursor)

        if self.orders:
            for order in self.orders:
                query.order_by(*order)
        else:
            for primary_key in self.primary_keys:
                query.order_by(primary_key)

        self.datas = query.commit()
        self.loader_state["query_count"] += 1
        self.loader_state["load_count"] += len(self.datas)
        self.compiled = False
        self.loaded = True

    def get(self):
        if not self.loaded:
            self.load()

        if not self.compiled:
            datas, self.datas = self.datas, []
            for data in datas:
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
                self.datas.append(values)
                self.last_data = data

        return super(DBLoader, self).get()

    def statistics(self):
        return {
            "querys": self.loader_state["query_count"],
            "rows": self.loader_state["load_count"]
        }