# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .db import DBOutputer
from ..valuers.valuer import LoadAllFieldsException

class DBUpdateInsertOutputer(DBOutputer):
    def __init__(self, *args, **kwargs):
        super(DBUpdateInsertOutputer, self).__init__(*args, **kwargs)

        self.load_data_keys = {}

    def load(self, datas):
        fields = set([])
        try:
            for name, valuer in self.schema.items():
                for key in valuer.get_fields():
                    fields.add(key)
        except LoadAllFieldsException:
            fields = []

        load_datas = []
        if len(self.primary_keys) == 1:
            data_count = len(datas)
            data_load_batch = int(data_count / 1000 + 1) if data_count % 1000 != 0 else int(data_count / 1000)
            for i in range(data_load_batch):
                query = self.db.query(self.name, self.primary_keys, list(fields))
                primary_values = []
                for data in datas[i * 1000: (i + 1) * 1000]:
                    primary_values.append(data[self.primary_keys[0]])

                query.filter_in(self.primary_keys[0], primary_values)

                load_datas.extend(query.commit())
                self.outputer_state["query_count"] += 1
        else:
            for data in datas:
                query = self.db.query(self.name, self.primary_keys, list(fields))
                for primary_key in self.primary_keys:
                    query.filter_eq(primary_key, data[primary_key])

                load_datas.extend(query.commit())
                self.outputer_state["query_count"] += 1
        self.outputer_state["load_count"] += len(load_datas)

        for data in load_datas:
            primary_key = self.get_data_primary_key(data)

            values = {}
            for key, field in self.schema.items():
                values[key] = field.clone().fill(data)
                setattr(values[key], "value_type_class", data.get(key).__class__)

            self.load_data_keys[primary_key] = values
            self.load_datas.append(values)

    def insert(self, datas):
        insert = self.db.insert(self.name, self.primary_keys, list(self.schema.keys()), datas)
        insert.commit()
        self.outputer_state["insert_count"] += 1

    def update(self, data, load_data):
        diff_data = {}
        for key, value in data.items():
            load_valuer = load_data[key]
            ovalue = load_valuer.get()
            if value != ovalue or getattr(load_valuer, "value_type_class") != value.__class__:
                diff_data[key] = ovalue
        if not diff_data:
            return

        update = self.db.update(self.name, self.primary_keys, list(self.schema.keys()), data, diff_data)
        for primary_key in self.primary_keys:
            update.filter_eq(primary_key, data[primary_key])
        update.commit()
        self.outputer_state["update_count"] += 1

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