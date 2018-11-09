# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from collections import defaultdict, OrderedDict
from .loader import Loader

class DBLoader(Loader):
    def __init__(self, db, name, *args, **kwargs):
        super(DBLoader, self).__init__(*args, **kwargs)

        self.db = db
        self.name = name
        self.querys = []

    def clone(self):
        loader = self.__class__(self.db, self.name, self.primary_keys)
        schema = OrderedDict()
        for key, valuer in self.schema.items():
            schema[key] = valuer.clone()
        loader.schema = schema
        loader.filters = [filter for filter in self.filters]
        loader.key_matchers = [matcher.clone() for matcher in self.key_matchers]
        return loader

    def load(self):
        if self.loaded:
            return

        fields = set([])
        if not self.key_matchers:
            for key, exp, value in self.filters:
                if key: fields.add(key)

            for name, valuer in self.schema.items():
                for field in valuer.get_fields():
                    fields.add(field)

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

        for primary_key in self.primary_keys:
            query.order_by(primary_key)
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

            self.data_keys[primary_key] = values
            self.datas.append(values)

        self.querys.append(query)
        self.loaded = True

    def statistics(self):
        return {
            "querys": len(self.querys),
            "rows": len(self.datas)
        }