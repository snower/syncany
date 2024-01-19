# -*- coding: utf-8 -*-
# 18/8/13
# create by: snower

import os
import datetime
from ..taskers.context import TaskerContext
from ..taskers.iterator import TaskerDataIterator, TaskerFileIterator
from ..utils import human_repr_object, sorted_by_keys
from .database import Cmper, QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase


class CsvFileNotFound(Exception):
    pass


class CsvQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(CsvQueryBuilder, self).__init__(*args, **kwargs)

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

    def open_file(self, name):
        if not name:
            raise CsvFileNotFound()
        names = name.split(".")
        if len(names) < 2:
            raise CsvFileNotFound()
        if names[1][:1] == "&":
            return None
        filename = os.path.join(self.db.config["path"], ".".join(names[1:]))
        if os.path.exists(filename):
            return open(filename, "r", newline='', encoding=self.db.config.get("encoding", "utf-8"))
        return None

    def csv_read(self, fp, descriptions, limit=0):
        import csv
        reader = csv.reader(fp, dialect=self.db.config.get("dialect", "excel"), quotechar=self.db.config.get("quotechar", '"'),
                            delimiter=self.db.config.get("delimiter", ','))
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

    def commit(self):
        tasker_context, iterator_name, datas = TaskerContext.current(), None, None
        if self.name not in self.db.csvs and not self.query and tasker_context and \
                (not self.orders or not tasker_context.tasker.config["orders"]) and self.limit:
            iterator_name = "csv::" + self.name
            iterator = tasker_context.get_iterator(iterator_name)
            if not iterator or iterator.offset != self.limit[0]:
                fp = self.open_file(self.name)
                if fp is not None:
                    iterator = TaskerFileIterator(fp, [])
                    tasker_context.add_iterator(iterator_name, iterator)
            if iterator:
                datas = self.csv_read(iterator.fp, iterator.fields, self.limit[1] - self.limit[0])
                iterator.offset += len(datas)
                return datas

        if self.limit and (self.query or self.orders):
            tasker_context = TaskerContext.current()
            if tasker_context:
                iterator_name = "csv::" + self.name
                iterator = tasker_context.get_iterator(iterator_name)
                if iterator and iterator.offset == self.limit[0]:
                    datas, iterator.offset = iterator.datas, self.limit[1]

        if not datas:
            if self.name not in self.db.csvs:
                fp = self.open_file(self.name)
                if fp is None:
                    return []
                try:
                    load_datas = self.csv_read(fp, [], 0)
                finally:
                    fp.close()
            else:
                csv_file = self.db.ensure_open_file(self.name)
                load_datas = csv_file.datas[:] if not self.query else csv_file.datas

            if not self.query:
                datas = load_datas
            else:
                datas = []
                for data in load_datas:
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
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("csv::" + self.name)

    def verbose(self):
        return "datas(%d): \n%s" % (len(self.datas), human_repr_object(self.datas))


class CsvUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(CsvUpdateBuilder, self).__init__(*args, **kwargs)

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
        csv_file = self.db.ensure_open_file(self.name)
        csv_file.fields = self.fields
        datas = []
        for data in csv_file.datas:
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

        csv_file.datas = datas
        csv_file.changed = True
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("csv::" + self.name)
        return datas

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query]),
            human_repr_object(self.diff_data))


class CsvDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(CsvDeleteBuilder, self).__init__(*args, **kwargs)

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
        csv_file = self.db.ensure_open_file(self.name)
        datas = []
        for data in csv_file.datas:
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

        csv_file.datas = datas
        csv_file.changed = True
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("csv::" + self.name)
        return datas

    def verbose(self):
        return "filters: %s" % human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query])


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
        "encoding": os.environ.get("SYNCANYENCODING", "utf-8"),
        "dialect": "excel",
        "quotechar": '"',
        "delimiter": ",",
        "datetime_format": None,
        "date_format": "%Y-%m-%d",
        "time_format": "%H:%M:%S"
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
        reader = csv.reader(fp, dialect=self.config.get("dialect", "excel"), quotechar=self.config.get("quotechar", '"'),
                            delimiter=self.config.get("delimiter", ','))
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
        writer = csv.writer(fp, dialect=self.config.get("dialect", "excel"), quotechar=self.config.get("quotechar", '"'),
                            delimiter=self.config.get("delimiter", ','), quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(fields)

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
            return value

        for data in csv_file.datas:
            data = [format_field_value(data[field]) for field in fields]
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

                fp = open(fileno, "r", newline='', encoding=self.config.get("encoding", "utf-8"), closefd=False)
                self.csvs[name] = self.read_file(name, fileno, fp)
                return self.csvs[name]

            filename = os.path.join(self.config["path"], ".".join(names[1:]))
            if os.path.exists(filename):
                with open(filename, "r", newline='', encoding=self.config.get("encoding", "utf-8")) as fp:
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
                with open(csv_file.filename, "w", newline='', encoding=self.config.get("encoding", "utf-8")) as fp:
                    self.write_file(fp, csv_file)
            else:
                if csv_file.filename == 0:
                    continue
                fp = open(csv_file.filename, "w", newline='', encoding=self.config.get("encoding", "utf-8"), closefd=False)
                self.write_file(fp, csv_file)
            csv_file.changed = False

    def close(self):
        self.flush()
        self.csvs = {}

    def verbose(self):
        return "%s<%s>" % (self.name, self.config["path"])