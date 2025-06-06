# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import math
from .db import DBOutputer, LoadDataValue
from ..valuers.valuer import LoadAllFieldsException


class DBUpdateOutputer(DBOutputer):
    def __init__(self, *args, **kwargs):
        self.join_batch = kwargs.pop("join_batch", 10000) or 0xffffffff
        super(DBUpdateOutputer, self).__init__(*args, **kwargs)

        self.load_data_keys = {}
        self.bulk_update_datas = {} if self.primary_keys and len(self.primary_keys) == 1 else None

    def clone(self):
        outputer = super(DBUpdateOutputer, self).clone()
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
        if self.bulk_update_datas is not None:
            if self.add_bulk_update_data(self.primary_keys[0], data, diff_data):
                return
            self.bulk_update_datas = None
        update = self.db.update(self.name, self.primary_keys, list(self.schema.keys()), data, diff_data)
        for primary_key in self.primary_keys:
            update.filter_eq(primary_key, data[primary_key])
        update.commit()
        self.outputer_state["update_count"] += 1

    def add_bulk_update_data(self, primary_key, data, diff_data):
        primary_value = data.pop(primary_key)
        try:
            data_update_key = tuple(((key, value) for key, value in data.items()))
            if data_update_key not in self.bulk_update_datas:
                self.bulk_update_datas[data_update_key] = (primary_key, data, diff_data, [])
            self.bulk_update_datas[data_update_key][3].append(primary_value)
        except TypeError:
            data[primary_key] = primary_value
            return False
        return True

    def execute_bulk_update(self):
        try:
            for primary_key, data, diff_data, primary_values in self.bulk_update_datas.values():
                if len(primary_values) == 1:
                    update = self.db.update(self.name, self.primary_keys, list(self.schema.keys()), data, diff_data)
                    update.filter_eq(primary_key, primary_values[0])
                    update.commit()
                    self.outputer_state["update_count"] += 1
                else:
                    for i in range(math.ceil(float(len(primary_values)) / float(self.join_batch))):
                        update = self.db.update(self.name, self.primary_keys, list(self.schema.keys()), data, diff_data)
                        update.filter_in(primary_key, primary_values[i * self.join_batch: (i + 1) * self.join_batch])
                        update.commit()
                        self.outputer_state["update_count"] += 1
        finally:
            self.bulk_update_datas = {}

    def store(self, datas):
        super(DBUpdateOutputer, self).store(datas)
        if not datas:
            return
        self.load(datas)

        for data in datas:
            primary_key = self.get_data_primary_key(data)
            if primary_key in self.load_data_keys:
                self.update(data, self.load_data_keys[primary_key])
        if self.bulk_update_datas:
            self.execute_bulk_update()