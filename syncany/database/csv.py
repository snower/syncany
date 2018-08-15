# -*- coding: utf-8 -*-
# 18/8/13
# create by: snower

import os
import csv
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

class CsvFileNotFound(Exception):
    pass

class CsvQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(CsvQueryBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        csv_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in csv_file.datas:
            succed = True
            for (key, exp), (value, cmp) in self.query.items():
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if succed:
                datas.append(data)

        if self.orders:
            datas = sorted(datas, key =  self.orders[0][0], reverse = True if self.orders[0][1] < 0 else False)

        return datas

class CsvInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(CsvInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        csv_file = self.db.ensure_open_file(self.name)
        csv_file.datas.extend(self.datas)

class CsvUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(CsvUpdateBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def commit(self):
        csv_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in csv_file.datas:
            succed = True
            for (key, exp), (value, cmp) in self.query.items():
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if succed:
                datas.append(self.update)
            else:
                datas.append(data)

        csv_file.datas = datas
        return datas

class CsvDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(CsvDeleteBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def commit(self):
        csv_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in csv_file.datas:
            succed = True
            for (key, exp), (value, cmp) in self.query.items():
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if not succed:
                datas.append(data)

        csv_file.datas = datas
        return datas

class CsvFile(object):
    def __init__(self, name, filename, datas):
        self.name = name
        self.filename = filename
        self.datas = datas

    def get_fields(self):
        fields = None
        for data in self.datas:
            if fields is None:
                fields = set(data.keys())
            else:
                fields = fields & set(data.keys())
        return tuple(fields) if fields else tuple()

class CsvDB(DataBase):
    DEFAULT_CONFIG = {
        "path": "./",
    }

    def __init__(self, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        all_config["path"] = os.path.abspath(all_config["path"])

        super(CsvDB, self).__init__(all_config)

        self.csvs = {}

    def ensure_open_file(self, name):
        if not name:
            raise CsvFileNotFound()

        if name not in self.csvs:
            names = name.split(".")
            if len(names) < 2:
                raise CsvFileNotFound()

            filename = os.path.join(self.config["path"], ".".join(names[1:]))
            if os.path.exists(filename):
                with open(filename, "r", newline='') as fp:
                    reader = csv.reader(fp, quotechar='"')
                    descriptions, datas = [], []
                    for row in reader:
                        if not descriptions:
                            descriptions = row
                        else:
                            datas.append({descriptions[i]: row[i] for i in range(len(descriptions))})
                    self.csvs[name] = CsvFile(name, filename, datas)
            else:
                self.csvs[name] = CsvFile(name, filename, [])

        return self.csvs[name]

    def query(self, name, primary_keys = None, *fields):
        return CsvQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys = None, datas = None):
        return CsvInsertBuilder(self, name, primary_keys, datas)

    def update(self, name, primary_keys = None, **update):
        return CsvUpdateBuilder(self, name, primary_keys, update)

    def delete(self, name, primary_keys = None):
        return CsvDeleteBuilder(self, name, primary_keys)

    def close(self):
        if self.csvs:
            for name, csv_file in self.csvs.items():
                fields = csv_file.get_fields()
                with open(csv_file.filename, "w", newline='') as fp:
                    writer = csv.writer(fp, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                    writer.writerow(fields)

                    for data in csv_file.datas:
                        data = [data[field] for field in fields]
                        writer.writerow(data)

        self.csvs = {}