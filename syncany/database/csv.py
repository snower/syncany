# -*- coding: utf-8 -*-
# 18/8/13
# create by: snower

import os
from collections import OrderedDict
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

    def filter_limit(self, count, start=None):
        if start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        csv_file = self.db.ensure_open_file(self.name)
        index, datas = 0, []
        for data in csv_file.datas:
            if self.limit and (index < self.limit[0] or index > self.limit[1]):
                continue

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
                index += 1

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
        csv_file.fields = self.fields
        csv_file.datas.extend(self.datas)
        csv_file.changed = True

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
        csv_file.fields = self.fields
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
        csv_file.changed = True
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
        csv_file.changed = True
        return datas

class CsvFile(object):
    def __init__(self, name, filename, datas):
        self.name = name
        self.filename = filename
        self.fields = ()
        self.datas = datas
        self.changed = False

    def get_fields(self):
        if self.fields:
            return self.fields

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
                with open(filename, "r", newline='', encoding="utf-8") as fp:
                    reader = csv.reader(fp, quotechar='"')
                    descriptions, datas = [], []
                    for row in reader:
                        if not descriptions:
                            descriptions = row
                        else:
                            data = OrderedDict()
                            for i in range(len(descriptions)):
                                data[descriptions[i]] = row[i]
                            datas.append(data)
                    self.csvs[name] = CsvFile(name, filename, datas)
            else:
                self.csvs[name] = CsvFile(name, filename, [])

        return self.csvs[name]

    def query(self, name, primary_keys = None, fields = ()):
        return CsvQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys = None, fields = (), datas = None):
        return CsvInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys = None, fields = (), update = None):
        return CsvUpdateBuilder(self, name, primary_keys, fields, update)

    def delete(self, name, primary_keys = None):
        return CsvDeleteBuilder(self, name, primary_keys)

    def close(self):
        if self.csvs:
            for name, csv_file in self.csvs.items():
                if not csv_file.changed:
                    continue

                fields = csv_file.get_fields()
                with open(csv_file.filename, "w", newline='', encoding="utf-8") as fp:
                    writer = csv.writer(fp, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                    writer.writerow(fields)

                    for data in csv_file.datas:
                        data = [data[field] for field in fields]
                        writer.writerow(data)

        self.csvs = {}