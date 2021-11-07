# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

import os
import csv
import json
from ..utils import print_object, get_rich, human_repr_object, human_format_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase


class TextLineQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(TextLineQueryBuilder, self).__init__(*args, **kwargs)

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

    def text_read(self, fp):
        datas = []
        try:
            for line in fp.readline():
                if not line:
                    return datas
                line = line.strip()
                if not line:
                    continue
                datas.append({"line": line})
        except KeyboardInterrupt:
            return datas
        return datas

    def csv_read(self, fp):
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
        return datas

    def json_read(self, fp):
        datas = []
        try:
            for line in fp.readline():
                if not line:
                    return datas
                try:
                    datas.append(json.loads(line, encoding="utf-8"))
                except:
                    continue
        except KeyboardInterrupt:
            return datas
        return datas

    def commit(self):
        fileinfo = self.name.split(".")
        if len(fileinfo) < 2:
            return []

        if fileinfo[1][:1] == "&":
            fileno = int(fileinfo[1][1:])
            if fileno < 0 or fileno in (1, 2):
                return []

            fp = open(fileno, "r", newline='', encoding="utf-8", closefd=False)
            if self.db.config.get("format") == "csv":
                rdatas = self.csv_read(fp)
            elif self.db.config.get("format") == "json":
                rdatas = self.json_read(fp)
            else:
                rdatas = self.text_read(fp)
        else:
            filename = os.path.join(self.db.config.get("path", "/"), ".".join(fileinfo[1:]))
            if not os.path.exists(filename):
                return []
            with open(filename, "r", newline='', encoding="utf-8") as fp:
                if self.db.config.get("format") == "csv":
                    rdatas = self.csv_read(fp)
                elif self.db.config.get("format") == "json":
                    rdatas = self.json_read(fp)
                else:
                    rdatas = self.text_read(fp)

        if not self.query:
            datas = rdatas
            if self.limit:
                datas = datas[self.limit[0]: self.limit[1]]
        else:
            index, datas = 0, []
            for data in rdatas:
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


class TextLineInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(TextLineInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def print_write(self, fp):
        if self.db.rich:
            self.db.rich.get_console().print(human_format_object(self.datas), markup=False)
        else:
            print_object(human_format_object(self.datas))

    def rich_write(self, fp):
        from rich.table import Table

        if self.db.rich is None:
            raise ImportError("rich>=9.11.1 is required")

        table = Table(show_header=True, collapse_padding=True, expand=True, highlight=True)
        for field in self.fields:
            table.add_column(field)
        for data in self.datas:
            table.add_row(*(str(data[field]) for field in self.fields))
        self.db.rich.print(table, file=fp)

    def text_write(self, fp):
        if len(self.fields) != 1 or self.fields[0] != "line":
            fp.write("\t".join(self.fields) + "\n")

            for data in self.datas:
                data = [str(data[field]) for field in self.fields]
                fp.write("\t".join(data) + "\n")
        else:
            for data in self.datas:
                fp.write(data["line"] + "\n")
        fp.flush()

    def csv_write(self, fp):
        writer = csv.writer(fp, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(self.fields)

        for data in self.datas:
            data = [data[field] for field in self.fields]
            writer.writerow(data)
        fp.flush()

    def json_write(self, fp):
        for data in self.datas:
            data = {field: data[field] for field in self.fields}
            fp.write(json.dumps(data, ensure_ascii=False, default=str))
            fp.write("\n")
        fp.flush()

    def commit(self):
        fileinfo = self.name.split(".")
        if len(fileinfo) < 2:
            return

        if fileinfo[1][:1] == "&":
            fileno = int(fileinfo[1][1:])
            if fileno <= 0:
                return
            fp = open(fileno, "w", newline='', encoding="utf-8", closefd=False)
            if self.db.config.get("format") == "csv":
                self.csv_write(fp)
            elif self.db.config.get("format") == "json":
                self.json_write(fp)
            elif self.db.config.get("format") == "richtable":
                self.rich_write(fp)
            elif fileno == 1 and self.db.config.get("format") == "print":
                self.print_write(fp)
            elif self.db.rich:
                self.rich_write(fp)
            else:
                self.text_write(fp)
            return

        filename = os.path.join(self.db.config.get("path", "/"), ".".join(fileinfo[1:]))
        if not os.path.exists(filename):
            return []
        with open(filename, "w", newline='', encoding="utf-8") as fp:
            if self.db.config.get("format") == "csv":
                self.csv_write(fp)
            elif self.db.config.get("format") == "json":
                self.json_write(fp)
            elif self.db.config.get("format") == "richtable":
                self.rich_write(fp)
            else:
                self.text_write(fp)

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)


class TextLineUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(TextLineUpdateBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        pass

    def filter_gte(self, key, value):
        pass

    def filter_lt(self, key, value):
        pass

    def filter_lte(self, key, value):
        pass

    def filter_eq(self, key, value):
        pass

    def filter_ne(self, key, value):
        pass

    def filter_in(self, key, value):
        pass

    def commit(self):
        return []


class TextLineDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(TextLineDeleteBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        pass

    def filter_gte(self, key, value):
        pass

    def filter_lt(self, key, value):
        pass

    def filter_lte(self, key, value):
        pass

    def filter_eq(self, key, value):
        pass

    def filter_ne(self, key, value):
        pass

    def filter_in(self, key, value):
        pass

    def commit(self):
        return []


class TextLineDB(DataBase):
    rich = None

    def __init__(self, manager, config):
        super(TextLineDB, self).__init__(manager, dict(**config))

        self.rich = get_rich()

    def query(self, name, primary_keys=None, fields=()):
        return TextLineQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return TextLineInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return TextLineUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return TextLineDeleteBuilder(self, name, primary_keys)