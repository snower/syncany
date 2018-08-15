# -*- coding: utf-8 -*-
# 18/8/13
# create by: snower

import os
import json
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

class JsonFileNotFound(Exception):
    pass

class JsonQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(JsonQueryBuilder, self).__init__(*args, **kwargs)

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
        json_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in json_file.datas:
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

class JsonInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(JsonInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        json_file = self.db.ensure_open_file(self.name)
        json_file.datas.extend(self.datas)

class JsonUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(JsonUpdateBuilder, self).__init__(*args, **kwargs)

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
        json_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in json_file.datas:
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

        json_file.datas = datas
        return datas

class JsonDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(JsonDeleteBuilder, self).__init__(*args, **kwargs)

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
        json_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in json_file.datas:
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

        json_file.datas = datas
        return datas

class JsonFile(object):
    def __init__(self, name, filename, datas):
        self.name = name
        self.filename = filename
        self.datas = datas

class JsonDB(DataBase):
    DEFAULT_CONFIG = {
        "path": "./",
    }

    def __init__(self, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        all_config["path"] = os.path.abspath(all_config["path"])

        super(JsonDB, self).__init__(all_config)

        self.jsons = {}

    def ensure_open_file(self, name):
        if not name:
            raise JsonFileNotFound()

        if name not in self.jsons:
            names = name.split(".")
            if len(names) < 2:
                raise JsonFileNotFound()

            filename = os.path.join(self.config["path"], ".".join(names[1:]))
            if os.path.exists(filename):
                with open(filename, "r") as fp:
                    datas = json.load(fp)
                    if not isinstance(datas, list):
                        datas = [datas]
                    self.jsons[name] = JsonFile(name, filename, datas)
            else:
                self.jsons[name] = JsonFile(name, filename, [])

        return self.jsons[name]

    def query(self, name, primary_keys = None, *fields):
        return JsonQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys = None, datas = None):
        return JsonInsertBuilder(self, name, primary_keys, datas)

    def update(self, name, primary_keys = None, **update):
        return JsonUpdateBuilder(self, name, primary_keys, update)

    def delete(self, name, primary_keys = None):
        return JsonDeleteBuilder(self, name, primary_keys)

    def close(self):
        if self.jsons:
            for name, json_file in self.jsons.items():
                with open(json_file.filename, "w") as fp:
                    json.dump(json_file.datas, fp, default=str, indent = 4, ensure_ascii = False, sort_keys = True)

        self.jsons = {}