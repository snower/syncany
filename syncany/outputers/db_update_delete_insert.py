# -*- coding: utf-8 -*-
# 18/11/18
# create by: snower

import math
from collections import defaultdict
from .db import DBOutputer
from ..valuers.valuer import LoadAllFieldsException

class DBUpdateDeleteInsertOutputer(DBOutputer):
    def __init__(self, *args, **kwargs):
        super(DBUpdateDeleteInsertOutputer, self).__init__(*args, **kwargs)

        self.load_data_keys = {}

    def load(self):
        fields = set([])
        try:
            for name, valuer in self.schema.items():
                for key in valuer.get_fields():
                    fields.add(key)
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

            getattr(query, "filter_%s" % exp)(key, value)

        if self.current_cursor:
            query.filter_cursor(*self.current_cursor)

        datas = query.commit()
        for data in datas:
            primary_key = self.get_data_primary_key(data)

            values = {}
            for key, field in self.schema.items():
                values[key] = field.clone().fill(data)
                setattr(values[key], "value_type_class", data.get(key).__class__)

            self.load_data_keys[primary_key] = values
            self.load_datas.append(values)
        self.outputer_state["query_count"] += 1
        self.outputer_state["load_count"] += len(datas)

    def insert(self, datas):
        if self.insert_batch > 0:
            for i in range(math.ceil(float(len(datas)) / float(self.insert_batch))):
                insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()),
                                        datas[i * self.insert_batch: (i + 1) * self.insert_batch])
                insert.commit()
                self.outputer_state["insert_count"] += 1
        else:
            insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()), datas)
            insert.commit()
            self.outputer_state["insert_count"] += 1

    def update(self, data, load_data):
        diff_data, require_update = {}, False
        for key, value in data.items():
            load_valuer = load_data[key]
            ovalue = load_valuer.get()
            if value != ovalue or getattr(load_valuer, "value_type_class") != value.__class__:
                diff_data[key] = ovalue
                option = self.schema[key].option
                if option and option.changed_require_update:
                    continue
                require_update = True

        if not require_update:
            return
        update = self.db.update(self.name, self.primary_keys, list(self.schema.keys()), data, diff_data)
        for primary_key in self.primary_keys:
            update.filter_eq(primary_key, data[primary_key])
        update.commit()
        self.outputer_state["update_count"] += 1

    def remove(self, datas):
        if len(self.primary_keys) == 1:
            primary_key_datas = []
            for data in datas:
                primary_key_datas.append(data[self.primary_keys[0]])
            delete = self.db.delete(self.name, self.primary_keys)
            delete.filter_in(self.primary_keys[0], primary_key_datas)
            delete.commit()
            self.outputer_state["delete_count"] += 1
        else:
            for data in datas:
                delete = self.db.delete(self.name, self.primary_keys)
                for primary_key in self.primary_keys:
                    delete.filter_eq(primary_key, data[primary_key])
                delete.commit()
                self.outputer_state["delete_count"] += 1

    def store(self, datas):
        super(DBUpdateDeleteInsertOutputer, self).store(datas)
        self.load()

        insert_datas = []
        update_datas = {}
        delete_datas = []

        for data in datas:
            primary_key = self.get_data_primary_key(data)
            if primary_key in self.load_data_keys:
                self.update(data, self.load_data_keys[primary_key])
                update_datas[primary_key] = data
            else:
                insert_datas.append(data)

        for data in self.load_datas:
            data = {key: valuer.get() for key, valuer in data.items()}
            primary_key = self.get_data_primary_key(data)
            if primary_key in update_datas:
                continue

            delete_datas.append(data)

        if delete_datas:
            self.remove(delete_datas)

        if insert_datas:
            self.insert(insert_datas)