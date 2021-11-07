# -*- coding: utf-8 -*-
# 18/8/13
# create by: snower

import os
import json
from ..utils import human_repr_object
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

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def filter_cursor(self, last_data, offset, count):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        json_file = self.db.ensure_open_file(self.name)
        if not self.query:
            datas = json_file.datas
            if self.limit:
                datas = datas[self.limit[0]: self.limit[1]]
        else:
            index, datas = 0, []
            for data in json_file.datas:
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
            datas = sorted(datas, key=lambda x: x.get(self.orders[0][0]), reverse=True if self.orders[0][1] < 0 else False)
        return datas

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            self.limit,
            self.orders)


class JsonInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(JsonInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        json_file = self.db.ensure_open_file(self.name)
        json_file.datas.extend(self.datas)
        json_file.changed = True

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)


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
        json_file.changed = True
        return datas

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            human_repr_object(self.diff_data))


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
        json_file.changed = True
        return datas

    def verbose(self):
        return "filters: %s" % human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()])


class JsonFile(object):
    def __init__(self, name, filename, datas):
        self.name = name
        self.filename = filename
        self.datas = datas
        self.changed = False


class JsonDB(DataBase):
    DEFAULT_CONFIG = {
        "path": "./",
    }

    def __init__(self, manager, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        all_config["path"] = os.path.abspath(all_config["path"])

        super(JsonDB, self).__init__(manager, all_config)

        self.jsons = {}

    def read_file(self, fp, name, filename):
        datas = json.load(fp)
        if not isinstance(datas, list):
            datas = [datas]
        return JsonFile(name, filename, datas)

    def write_file(self, fp, json_file):
        json.dump(json_file.datas, fp, default=str, indent=4, ensure_ascii=False, sort_keys=True)
        fp.flush()

    def ensure_open_file(self, name):
        if not name:
            raise JsonFileNotFound()

        if name not in self.jsons:
            names = name.split(".")
            if len(names) < 2:
                raise JsonFileNotFound()

            if names[1][:1] == "&":
                fileno = int(names[1][1:])
                if fileno in (1, 2):
                    self.jsons[name] = JsonFile(name, fileno, [])
                    return self.jsons[name]

                fp = open(fileno, "r", closefd=False)
                self.jsons[name] = self.read_file(fp, name, fileno)
                return self.jsons[name]

            filename = os.path.join(self.config["path"], ".".join(names[1:]))
            if os.path.exists(filename):
                with open(filename, "r") as fp:
                    self.jsons[name] = self.read_file(fp, name, filename)
            else:
                self.jsons[name] = JsonFile(name, filename, [])
        return self.jsons[name]

    def query(self, name, primary_keys=None, fields=()):
        return JsonQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return JsonInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return JsonUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return JsonDeleteBuilder(self, name, primary_keys)

    def flush(self):
        if not self.jsons:
            return

        for name, json_file in self.jsons.items():
            if not json_file.changed:
                continue

            if isinstance(json_file.filename, str):
                with open(json_file.filename, "w") as fp:
                    self.write_file(fp, json_file)
            else:
                if json_file.filename == 0:
                    continue
                fp = open(json_file.filename, "w", closefd=False)
                self.write_file(fp, json_file)
            json_file.changed = False

    def close(self):
        self.flush()
        self.jsons = {}

    def verbose(self):
        return "%s<%s>" % (self.name, self.config["path"])