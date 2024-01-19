# -*- coding: utf-8 -*-
# 18/8/13
# create by: snower

import os
import json
import datetime
from ..utils import human_repr_object, sorted_by_keys
from ..taskers.context import TaskerContext
from ..taskers.iterator import TaskerDataIterator
from .database import Cmper, QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase


class JsonFileNotFound(Exception):
    pass


class JsonQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(JsonQueryBuilder, self).__init__(*args, **kwargs)

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

    def commit(self):
        tasker_context, iterator_name, datas = None, None, None
        if self.limit and (self.query or self.orders):
            tasker_context = TaskerContext.current()
            if tasker_context:
                iterator_name = "json::" + self.name
                iterator = tasker_context.get_iterator(iterator_name)
                if iterator and iterator.offset == self.limit[0]:
                    datas, iterator.offset = iterator.datas, self.limit[1]

        if not datas:
            json_file = self.db.ensure_open_file(self.name)
            if not self.query:
                datas = json_file.datas[:]
            else:
                datas = []
                for data in json_file.datas:
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
            if tasker_context and self.limit and (self.query or self.orders):
                tasker_context.add_iterator(iterator_name, TaskerDataIterator(datas, self.limit[1]))

        if self.limit:
            datas = datas[self.limit[0]: self.limit[1]]
        return datas

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query]),
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
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("json::" + self.name)

    def verbose(self):
        return "datas(%d): \n%s" % (len(self.datas), human_repr_object(self.datas))


class JsonUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(JsonUpdateBuilder, self).__init__(*args, **kwargs)

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

    def commit(self):
        json_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in json_file.datas:
            succed = True
            for key, exp, value, cmp in self.query:
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
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("json::" + self.name)
        return datas

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query]),
            human_repr_object(self.diff_data))


class JsonDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(JsonDeleteBuilder, self).__init__(*args, **kwargs)

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

    def commit(self):
        json_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in json_file.datas:
            succed = True
            for key, exp, value, cmp in self.query:
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
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("json::" + self.name)
        return datas

    def verbose(self):
        return "filters: %s" % human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query])


class JsonFile(object):
    def __init__(self, name, filename, datas):
        self.name = name
        self.filename = filename
        self.datas = datas
        self.changed = False


class JsonDB(DataBase):
    DEFAULT_CONFIG = {
        "path": "./",
        "encoding": os.environ.get("SYNCANYENCODING", "utf-8"),
        "datetime_format": None,
        "date_format": "%Y-%m-%d",
        "time_format": "%H:%M:%S"
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
        datetime_format = self.config["datetime_format"]
        date_format = self.config["date_format"]
        time_format = self.config["time_format"]
        def format_field_value(value):
            if isinstance(value, datetime.date):
                if isinstance(value, datetime.datetime):
                    return value.strftime(datetime_format) if datetime_format else value.isoformat()
                return value.strftime(date_format)
            if isinstance(value, datetime.time):
                return value.strftime(time_format)
            return str(value)

        json.dump(json_file.datas, fp, default=format_field_value, indent=4, ensure_ascii=False, sort_keys=True)
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

                fp = open(fileno, "r", encoding=self.config.get("encoding", "utf-8"), closefd=False)
                self.jsons[name] = self.read_file(fp, name, fileno)
                return self.jsons[name]

            filename = os.path.join(self.config["path"], ".".join(names[1:]))
            if os.path.exists(filename):
                with open(filename, "r", encoding=self.config.get("encoding", "utf-8")) as fp:
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
                with open(json_file.filename, "w", encoding=self.config.get("encoding", "utf-8")) as fp:
                    self.write_file(fp, json_file)
            else:
                if json_file.filename == 0:
                    continue
                fp = open(json_file.filename, "w", encoding=self.config.get("encoding", "utf-8"), closefd=False)
                self.write_file(fp, json_file)
            json_file.changed = False

    def close(self):
        self.flush()
        self.jsons = {}

    def is_dynamic_schema(self, name):
        return True

    def verbose(self):
        return "%s<%s>" % (self.name, self.config["path"])