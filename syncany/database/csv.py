# -*- coding: utf-8 -*-
# 18/8/13
# create by: snower

import os
from ..utils import human_repr_object
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
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def filter_cursor(self, last_data, offset, count):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        csv_file = self.db.ensure_open_file(self.name)
        if not self.query:
            datas = csv_file.datas
            if self.limit:
                datas = datas[self.limit[0]: self.limit[1]]
        else:
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
            datas = sorted(datas, key=lambda x: x.get(self.orders[0][0]), reverse=True if self.orders[0][1] < 0 else False)
        return datas

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            self.limit,
            self.orders)


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

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)


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

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            human_repr_object(self.diff_data))


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

    def verbose(self):
        return "filters: %s" % human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()])


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

    def __init__(self, manager, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        all_config["path"] = os.path.abspath(all_config["path"])

        super(CsvDB, self).__init__(manager, all_config)

        self.csvs = {}

    def read_file(self, name, filename, fp):
        import csv
        reader = csv.reader(fp, quotechar='"')
        descriptions, datas = [], []
        for row in reader:
            if not descriptions:
                descriptions = row
            else:
                data = {}
                for i in range(len(descriptions)):
                    data[descriptions[i]] = row[i]
                datas.append(data)
        return CsvFile(name, filename, datas)

    def write_file(self, fp, csv_file):
        import csv
        fields = csv_file.get_fields()
        writer = csv.writer(fp, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(fields)

        for data in csv_file.datas:
            data = [data[field] for field in fields]
            writer.writerow(data)
        fp.flush()

    def ensure_open_file(self, name):
        if not name:
            raise CsvFileNotFound()

        if name not in self.csvs:
            names = name.split(".")
            if len(names) < 2:
                raise CsvFileNotFound()

            if names[1][:1] == "&":
                fileno = int(names[1][1:])
                if fileno in (1, 2):
                    self.csvs[name] = CsvFile(name, fileno, [])
                    return self.csvs[name]

                fp = open(fileno, "r", newline='', encoding="utf-8", closefd=False)
                self.csvs[name] = self.read_file(name, fileno, fp)
                return self.csvs[name]

            filename = os.path.join(self.config["path"], ".".join(names[1:]))
            if os.path.exists(filename):
                with open(filename, "r", newline='', encoding="utf-8") as fp:
                    self.csvs[name] = self.read_file(name, filename, fp)
            else:
                self.csvs[name] = CsvFile(name, filename, [])
        return self.csvs[name]

    def query(self, name, primary_keys=None, fields=()):
        return CsvQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return CsvInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return CsvUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return CsvDeleteBuilder(self, name, primary_keys)

    def flush(self):
        if not self.csvs:
            return
        for name, csv_file in self.csvs.items():
            if not csv_file.changed:
                continue

            if isinstance(csv_file.filename, str):
                with open(csv_file.filename, "w", newline='', encoding="utf-8") as fp:
                    self.write_file(fp, csv_file)
            else:
                if csv_file.filename == 0:
                    continue
                fp = open(csv_file.filename, "w", newline='', encoding="utf-8", closefd=False)
                self.write_file(fp, csv_file)
            csv_file.changed = False

    def close(self):
        self.flush()
        self.csvs = {}

    def verbose(self):
        return "%s<%s>" % (self.name, self.config["path"])