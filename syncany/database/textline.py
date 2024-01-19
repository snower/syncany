# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

import datetime
import os
import csv
import json
from ..utils import print_object, get_rich, human_repr_object, human_format_object, sorted_by_keys
from ..taskers.context import TaskerContext
from ..taskers.iterator import TaskerFileIterator
from .database import Cmper, QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase


class TextLineSpliter(object):
    ESCAPE_CHARS = ['\a', '\b', '\f', '\n', '\r', '\t', '\v', '\\', '\'', '"', '\0']

    def __init__(self, sep=' ', escapes=('"', "'"), boundarys=None):
        self.sep = sep
        self.escapes = escapes
        self.boundarys = boundarys if boundarys else {"[": "]", "(": ")"}
        self.line_text = ""
        self.index = 0
        self.len = 0

    def next(self):
        self.index += 1

    def skip_escape(self, c):
        start_index = self.index
        self.next()
        while self.index < self.len:
            if self.line_text[self.index] != c:
                self.next()
                continue
            backslash = self.index - 1
            while backslash >= 0:
                if self.line_text[backslash] != '\\':
                    break
                backslash -= 1
            if (self.index - backslash + 1) % 2 != 0:
                self.next()
                continue
            self.next()
            return start_index, self.index, self.line_text[start_index: self.index]
        self.next()
        raise EOFError(self.line_text[start_index:])

    def read_util(self, cs, escape_chars=('"', "'")):
        start_index = self.index
        while self.index < self.len:
            if self.line_text[self.index] in escape_chars:
                self.skip_escape(self.line_text[self.index])
                continue
            if self.line_text[self.index: self.index + len(cs)] != cs:
                self.next()
                continue
            return start_index, self.index + len(cs) - 1, self.line_text[start_index: self.index + len(cs) - 1]
        raise EOFError(self.line_text[start_index:])

    def split(self, line_text, field_names=None):
        self.line_text = line_text.strip()
        self.index = 0
        self.len = len(self.line_text)

        fields, field_index = {}, 0
        start_index = self.index
        try:
            while self.index < self.len:
                if self.line_text[self.index] in self.escapes:
                    self.skip_escape(self.line_text[self.index])
                    continue
                if self.line_text[self.index] in self.boundarys:
                    self.read_util(self.boundarys[self.line_text[self.index]])
                    self.next()
                    continue
                if self.line_text[self.index] == self.sep:
                    field_name = "seg%d" % field_index
                    if not field_names or field_name in field_names:
                        fields[field_name] = self.line_text[start_index: self.index]
                    field_index += 1
                    self.next()
                    start_index = self.index
                    continue
                self.next()
        except EOFError:
            pass
        if start_index < self.index:
            field_name = "seg%d" % field_index
            if not field_names or field_name in field_names:
                fields[field_name] = self.line_text[start_index: self.index]
        return fields


class TextLineQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(TextLineQueryBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query.append((key, '>', value, Cmper.cmp_gt))

    def filter_gte(self, key, value):
        self.query.append((key, ">=", value, Cmper.cmp_gte))

    def filter_lt(self, key, value):
        self.query.append((key, "<", value, Cmper.cmp_lt))

    def filter_lte(self, key, value):
        self.query.append((key, "<=", value, Cmper.cmp_lte))

    def filter_eq(self, key, value):
        self.query.append((key, "==", value, Cmper.cmp_eq))

    def filter_ne(self, key, value):
        self.query.append((key, "!=", value, Cmper.cmp_ne))

    def filter_in(self, key, value):
        try:
            self.query.append((key, "in", set(value) if isinstance(value, list) else value, Cmper.cmp_in))
        except:
            self.query.append((key, "in", value, Cmper.cmp_in))

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def filter_cursor(self, last_data, offset, count, primary_orders=None):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def text_read(self, fp, limit=0):
        fields, datas = (set(self.fields) if self.fields else None), []
        try:
            if self.db.config.get("sep"):
                escapes = (c for c in self.db.config.get("escape", "\"'"))
                boundarys = self.db.config.get("boundary", "[]()")
                boundarys = {boundarys[i*2]: boundarys[i*2+1] for i in range(int(len(boundarys) / 2))}
                textline_spliter = TextLineSpliter(self.db.config["sep"][:1], escapes, boundarys)
                for line in fp:
                    data = textline_spliter.split(line, fields)
                    if not fields or "line" in fields:
                        data["line"] = line
                    datas.append(data)
                    if limit > 0 and len(datas) >= limit:
                        break
            else:
                for line in fp:
                    datas.append({"line": line})
                    if limit > 0 and len(datas) >= limit:
                        break
        except KeyboardInterrupt:
            return datas
        return datas

    def csv_read(self, fp, descriptions, limit=0):
        reader = csv.reader(fp, quotechar='"')
        fields, datas = (set(self.fields) if self.fields else None), []
        for row in reader:
            if not descriptions:
                descriptions.extend(row)
            else:
                data = {}
                for i in range(len(descriptions)):
                    if fields and descriptions[i] not in fields:
                        continue
                    data[descriptions[i]] = row[i]
                datas.append(data)
                if limit > 0 and len(datas) >= limit:
                    break
        return datas

    def json_read(self, fp, limit=0):
        datas = []
        try:
            for line in fp.readline():
                if not line:
                    return datas
                try:
                    datas.append(json.loads(line, encoding="utf-8"))
                    if limit > 0 and len(datas) >= limit:
                        break
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

            fp = open(fileno, "r", newline='', encoding=self.db.config.get("encoding", "utf-8"), closefd=False)
            if self.db.config.get("format") == "csv":
                rdatas = self.csv_read(fp, [])
            elif self.db.config.get("format") == "json":
                rdatas = self.json_read(fp)
            else:
                rdatas = self.text_read(fp)
        else:
            filename = os.path.join(self.db.config.get("path", "/"), ".".join(fileinfo[1:]))
            if not os.path.exists(filename):
                return []

            tasker_context = TaskerContext.current()
            if not self.query and tasker_context and (not self.orders or not tasker_context.tasker.config["orders"]) and self.limit:
                iterator_name = "textline::" + self.name
                iterator = tasker_context.get_iterator(iterator_name)
                if not iterator or iterator.offset != self.limit[0]:
                    iterator = TaskerFileIterator(open(filename, "r", newline='', encoding=self.db.config.get("encoding", "utf-8")), [])
                    tasker_context.add_iterator(iterator_name, iterator)
                if self.db.config.get("format") == "csv":
                    rdatas = self.csv_read(iterator.fp, iterator.fields, self.limit[1] - self.limit[0])
                elif self.db.config.get("format") == "json":
                    rdatas = self.json_read(iterator.fp, self.limit[1] - self.limit[0])
                else:
                    rdatas = self.text_read(iterator.fp, self.limit[1] - self.limit[0])
                iterator.offset += len(rdatas)
                return rdatas

            with open(filename, "r", newline='', encoding=self.db.config.get("encoding", "utf-8")) as fp:
                if self.db.config.get("format") == "csv":
                    rdatas = self.csv_read(fp, [])
                elif self.db.config.get("format") == "json":
                    rdatas = self.json_read(fp)
                else:
                    rdatas = self.text_read(fp)

        if not self.query:
            datas = rdatas
        else:
            datas = []
            for data in rdatas:
                succed = True
                for key, exp, value, cmp in self.query:
                    if key not in data:
                        succed = False
                        break
                    if not cmp(data[key], value):
                        succed = False
                        break
                if succed:
                    datas.append(data)

        if self.orders:
            datas = sorted_by_keys(datas, keys=[(key, True if direct < 0 else False)
                                                for key, direct in self.orders] if self.orders else None)
        if self.limit:
            datas = datas[self.limit[0]: self.limit[1]]
        return datas

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query]),
            self.limit,
            self.orders)


class TextLineInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(TextLineInsertBuilder, self).__init__(*args, **kwargs)

        self.datetime_format = self.db.config["datetime_format"]
        self.date_format = self.db.config["date_format"]
        self.time_format = self.db.config["time_format"]

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def format_field_value(self, value):
        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                return value.strftime(self.datetime_format)
            return value.strftime(self.date_format)
        if isinstance(value, datetime.time):
            return value.strftime(self.time_format)
        return value

    def format_field_string(self, value):
        return str(self.format_field_value(value))

    def print_write(self, fp):
        if self.db.rich:
            self.db.rich.get_console().print(human_format_object(self.datas), markup=False)
        else:
            print_object(human_format_object(self.datas))
        fp.flush()

    def rich_write(self, fp):
        if self.db.rich is None:
            raise ImportError("rich>=9.11.1 is required")
        try:
            from rich.table import Table
        except ImportError:
            raise ImportError("rich>=9.11.1 is required")

        table = Table(show_header=True, collapse_padding=True, expand=True, highlight=True)
        for field in self.fields:
            table.add_column(field)
        for data in self.datas:
            table.add_row(*(self.format_field_string(data[field]) for field in self.fields))
        self.db.rich.print(table, file=fp)
        fp.flush()

    def text_write(self, fp):
        sep = self.db.config.get("sep") or " "

        def format_field(value):
            value = self.format_field_string(value)
            if sep in value:
                return "'%s'" % self.format_field_string(value)
            return self.format_field_string(value)
        for data in self.datas:
            data = [format_field(data[field]) for field in self.fields]
            fp.write(sep.join(data) + "\n")
        fp.flush()

    def csv_write(self, fp):
        writer = csv.writer(fp, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(self.fields)

        for data in self.datas:
            data = [self.format_field_value(data[field]) for field in self.fields]
            writer.writerow(data)
        fp.flush()

    def json_write(self, fp):
        for data in self.datas:
            data = {field: data[field] for field in self.fields}
            fp.write(json.dumps(data, ensure_ascii=False, default=self.format_field_string))
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
            fp = open(fileno, "w", newline='', encoding=self.db.config.get("encoding", "utf-8"), closefd=False)
            if self.db.config.get("format") == "csv":
                self.csv_write(fp)
            elif self.db.config.get("format") == "json":
                self.json_write(fp)
            elif self.db.config.get("format") == "txt":
                self.text_write(fp)
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
        with open(filename, "w", newline='', encoding=self.db.config.get("encoding", "utf-8")) as fp:
            if self.db.config.get("format") == "csv":
                self.csv_write(fp)
            elif self.db.config.get("format") == "json":
                self.json_write(fp)
            elif self.db.config.get("format") == "txt":
                self.text_write(fp)
            elif self.db.config.get("format") == "richtable":
                self.rich_write(fp)
            else:
                self.text_write(fp)
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("textline::" + self.name)

    def verbose(self):
        return "datas(%d): \n%s" % (len(self.datas), human_repr_object(self.datas))


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
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("textline::" + self.name)
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
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("textline::" + self.name)
        return []


class TextLineDB(DataBase):
    DEFAULT_CONFIG = {
        "encoding": os.environ.get("SYNCANYENCODING", "utf-8"),
        "sep": None,
        "escape": "\"'",
        "boundary": "[]()",
        "datetime_format": "%Y-%m-%d %H:%M:%S",
        "date_format": "%Y-%m-%d",
        "time_format": "%H:%M:%S"
    }
    rich = None

    def __init__(self, manager, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        super(TextLineDB, self).__init__(manager, all_config)

        self.rich = get_rich()

    def query(self, name, primary_keys=None, fields=()):
        return TextLineQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return TextLineInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return TextLineUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return TextLineDeleteBuilder(self, name, primary_keys)