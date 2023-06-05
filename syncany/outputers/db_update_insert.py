# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import math
from .db import DBOutputer, LoadDataValue
from ..valuers.valuer import LoadAllFieldsException


class DBUpdateInsertOutputer(DBOutputer):
    def __init__(self, *args, **kwargs):
        self.join_batch = kwargs.pop("join_batch", 10000) or 0xffffffff
        super(DBUpdateInsertOutputer, self).__init__(*args, **kwargs)

        self.load_data_keys = {}

    def clone(self):
        outputer = super(DBUpdateInsertOutputer, self).clone()
        outputer.join_batch = self.join_batch
        return outputer

    def load(self, datas):
        fields = set([])
        try:
            for name, valuer in self.schema.items():
                for key in valuer.get_fields():
                    fields.add(key)
        except LoadAllFieldsException:
            fields = []

        self.load_datas, self.load_data_keys = [], {}
        for i in range(math.ceil(float(len(datas)) / float(self.join_batch))):
            current_datas = datas[i * self.join_batch: (i + 1) * self.join_batch]
            if not current_datas:
                break
            query = self.db.query(self.name, self.primary_keys, list(fields))
            primary_values = {primary_key: set([]) for primary_key in self.primary_keys}
            for data in current_datas:
                for primary_key in self.primary_keys:
                    primary_values[primary_key].add(data[primary_key])
            for primary_key in self.primary_keys:
                query.filter_in(primary_key, list(primary_values[primary_key]))

            query = self.load_datas.extend(query.commit())
            self.outputer_state["query_count"] += 1
        self.outputer_state["load_count"] += len(self.load_datas)

        for i in range(len(self.load_datas)):
            data, values = self.load_datas[i], {}
            primary_key = self.get_data_primary_key(data)
            for key, field in self.schema.items():
                values[key] = LoadDataValue(field.fill_get(data))
                setattr(values[key], "value_type_class", data.get(key).__class__)

            self.load_data_keys[primary_key] = values
            self.load_datas[i] = values

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

    def store(self, datas):
        super(DBUpdateInsertOutputer, self).store(datas)
        if not datas:
            return
        self.load(datas)

        insert_datas = []
        for data in datas:
            primary_key = self.get_data_primary_key(data)
            if primary_key in self.load_data_keys:
                self.update(data, self.load_data_keys[primary_key])
            else:
                insert_datas.append(data)

        if insert_datas:
            self.insert(insert_datas)
