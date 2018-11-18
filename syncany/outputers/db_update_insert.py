# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from collections import defaultdict
from .db import DBOutputer

class DBUpdateInsertOutputer(DBOutputer):
    def load(self, datas):
        fields = set([])
        for name, valuer in self.schema.items():
            for key in valuer.get_fields():
                fields.add(key)

        load_datas = []
        if len(self.primary_keys) == 1:
            for i in range(int(len(datas) / 1000.0 + 1)):
                query = self.db.query(self.name, self.primary_keys, list(fields))
                primary_values = []
                for data in datas[i * 1000: (i + 1) * 1000]:
                    primary_values.append(data[self.primary_keys[0]])

                query.filter_in(self.primary_keys[0], primary_values)

                load_datas.extend(query.commit())
                self.querys.append(query)
        else:
            for data in datas:
                query = self.db.query(self.name, self.primary_keys, list(fields))
                for primary_key in self.primary_keys:
                    query.filter_eq(primary_key, data[primary_key])

                load_datas.extend(query.commit())
                self.querys.append(query)

        for data in load_datas:
            primary_key = self.get_data_primary_key(data)

            values = {}
            for key, field in self.schema.items():
                values[key] = field.clone().fill(data)
                setattr(values[key], "value_type_class", data.get(key).__class__)

            self.load_data_keys[primary_key] = values
            self.load_datas.append(values)

    def insert(self, datas):
        for i in range(int(len(datas) / 500.0 + 1)):
            bdatas = datas[i * 500: (i + 1) * 500]
            if bdatas:
                insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()), bdatas)
                insert.commit()
                self.operators.append(insert)

    def update(self, data, load_data):
        eq =  True
        for key, value in data.items():
            load_valuer = load_data[key]
            if value != load_valuer.get() or getattr(load_valuer, "value_type_class") != value.__class__:
                eq = False
                break
        if eq:
            return

        update = self.db.update(self.name, self.primary_keys, list(self.schema.keys()), data)
        for primary_key in self.primary_keys:
            update.filter_eq(primary_key, data[primary_key])
        update.commit()
        self.operators.append(update)

    def store(self, datas):
        super(DBUpdateInsertOutputer, self).store(datas)
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