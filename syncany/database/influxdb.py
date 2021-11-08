# -*- coding: utf-8 -*-
# 2020/11/27
# create by: snower

import re
import datetime
import pytz
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase, DatabaseFactory

escape_chars_map = {
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\0": "\\0",
    "\a": "\\a",
    "\v": "\\v",
    "\\": "\\\\",
    "'": "\\'"
}


def escape_param(item):
    if item is None:
        return 0
    elif isinstance(item, datetime.datetime):
        if item.tzinfo and item.utcoffset().total_seconds() > 0:
            item = item.astimezone(pytz.UTC)
        return "'%s'" % item.strftime('%Y-%m-%dT%H:%M:%SZ')
    elif isinstance(item, datetime.date):
        return "'%s'" % item.strftime('%Y-%m-%d')
    elif isinstance(item, str):
        return "'%s'" % ''.join(escape_chars_map.get(c, c) for c in item)
    elif isinstance(item, list):
        return "(%s)" % ', '.join(str(escape_param(x)) for x in item)
    return item


def escape_args(args):
    if isinstance(args, (list, set, tuple)):
        return tuple(escape_param(arg) for arg in args)
    elif isinstance(args, dict):
        return {key: escape_param(val) for (key, val) in args.items()}
    else:
        return escape_param(args)


class InfluxDBQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(InfluxDBQueryBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []
        self.sql = None

        db_name = self.name.split(".")
        if len(db_name) > 1:
            self.table_name = ".".join(db_name[1:])
        else:
            self.table_name = db_name[0]

    def filter_gt(self, key, value):
        self.query.append('"' + key + '">%s')
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('"' + key + '">=%s')
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('"' + key + '"<%s')
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('"' + key + '"<=%s')
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('"' + key + '"=%s')
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('"' + key + '"!=%s')
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('"' + key + '" in %s')
        self.query_values.append(value)

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def filter_cursor(self, last_data, offset, count):
        if len(self.primary_keys) == 1 and self.primary_keys[0] in last_data:
            self.query.append('"' + self.primary_keys[0] + '">%s')
            self.query_values.append(last_data[self.primary_keys[0]])
        else:
            self.limit = (offset, count)

    def order_by(self, key, direct=1):
        self.orders.append(('"' + key + ('" ASC' if direct else '" DESC')))

    def format_table(self):
        for virtual_table in self.db.virtual_tables:
            if virtual_table.get("name_match"):
                name_match = re.compile(virtual_table.get("name_match"))
                if not name_match.match(self.table_name):
                    continue
                if isinstance(virtual_table["query"], list):
                    virtual_table["query"] = " ".join(virtual_table["query"])
                sql = virtual_table['query'].replace('"%s"' % virtual_table["name"], '"%s"' % self.table_name)
            elif virtual_table["name"] != self.table_name:
                continue
            else:
                if isinstance(virtual_table["query"], list):
                    virtual_table["query"] = " ".join(virtual_table["query"])
                sql = virtual_table['query']
            return '(%s) "virtual_%s"' % (sql, self.table_name), virtual_table.get("args", []), True
        return ('"%s"' % self.table_name), [], False

    def format_query(self, db_name, virtual_args):
        if not virtual_args:
            query, query_values = [], []
            for i in range(len(self.query)):
                if self.query[i][-6:] == " in %s":
                    in_querys = []
                    for qv in self.query_values[i]:
                        in_querys.append(self.query[i][:-6] + "=%s")
                        query_values.append(qv)
                    query.append("(" + " OR ".join(in_querys) + ")")
                else:
                    query.append(self.query[i])
                    query_values.append(self.query_values[i])
            return db_name, (" AND ".join(query) if query else ""), query_values

        query, query_values, virtual_query, virtual_values = [], [], {}, []
        for arg in virtual_args:
            if isinstance(arg, str):
                virtual_q = '"' + arg[0] + '"=%s'
            else:
                if arg[1] == "in":
                    virtual_q = "`" + arg[0] + "` " + arg[1] + " %s"
                else:
                    virtual_q = "`" + arg[0] + "`" + arg[1] + "%s"
            for i in range(len(self.query)):
                if self.query[i] == virtual_q:
                    virtual_query[self.query[i]] = self.query_values[i]
                    if isinstance(arg, str):
                        db_name = db_name.replace('"' + arg + '"', '"' + self.query_values[i] + '"')
                    else:
                        virtual_values.append(self.query_values[i])
                    break
            if virtual_q in virtual_query:
                continue
            if isinstance(arg, str):
                continue
            virtual_values.append(arg[2] if len(arg) >= 3 else None)

        for i in range(len(self.query)):
            if self.query[i] in virtual_query:
                continue
            if self.query[i][-6:] == " in %s":
                in_querys = []
                for qv in self.query_values[i]:
                    in_querys.append(self.query[i][:-6] + "=%s")
                    query_values.append(qv)
                query.append("(" + " OR ".join(in_querys) + ")")
            else:
                query.append(self.query[i])
                query_values.append(self.query_values[i])
        return db_name, (" AND ".join(query) if query else ""), (virtual_values + query_values)

    def commit(self):
        db_name, virtual_args, is_virtual = self.format_table()
        db_name, query, query_values = self.format_query(db_name, virtual_args)

        if self.fields and not is_virtual:
            fields = []
            for field in self.fields:
                fields.append('"' + field + '"')
            fields = ", ".join(fields)
        else:
            fields = "*"

        where = (" WHERE " + query) if query else ""
        order_by = (" ORDER BY " + ",".join(self.orders)) if self.orders else ""
        limit = (" LIMIT %s%s" % (("%s," % self.limit[0]) if self.limit[0] else "", self.limit[1])) if self.limit else ""
        sql = "SELECT %s FROM %s%s%s%s" % (fields, db_name, where, order_by, limit)
        connection = self.db.ensure_connection()
        try:
            result = connection.query(sql % escape_args(query_values))
            return list(result.get_points())
        finally:
            self.db.release_connection()
            self.sql = (sql, query_values)

    def verbose(self):
        if isinstance(self.sql, tuple):
            if "\n" in self.sql[0]:
                return "sql: \n%s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class InfluxDBInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(InfluxDBInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def get_fields(self):
        fields = None
        for data in self.datas:
            if fields is None:
                fields = set(data.keys())
            else:
                fields = fields & set(data.keys())
        return tuple(fields) if fields else tuple()

    def commit(self):
        fields = self.get_fields() if not self.fields else self.fields
        db_name = self.name.split(".")
        table_name = ".".join(db_name[1:]) if len(db_name) > 1 else db_name[0]
        if table_name in self.db.tables:
            tags = self.db.tables[table_name].get("tags")
        else:
            tags = None

        datas = []
        for data in self.datas:
            json_body = {"measurement": table_name, "tags": {}, "time": None, "fields": {}}
            for field in fields:
                value = escape_param(data[field]) if isinstance(data[field], datetime.date) else data[field]
                if field == "time":
                    json_body["time"] = value
                elif tags:
                    if field in tags:
                        json_body["tags"][field] = value
                    else:
                        json_body["fields"][field] = value
                else:
                    if isinstance(data[field], (str, datetime.date)) or field in self.primary_keys:
                        json_body["tags"][field] = value
                    else:
                        json_body["fields"][field] = value
            if not json_body["time"]:
                json_body["time"] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            datas.append(json_body)

        connection = self.db.ensure_connection()
        try:
            return connection.write_points(datas)
        finally:
            self.db.release_connection()

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)


class InfluxDBUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(InfluxDBUpdateBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []

    def filter_gt(self, key, value):
        self.query.append('"' + key + '">%s')
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('"' + key + '">=%s')
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('"' + key + '"<%s')
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('"' + key + '"<=%s')
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('"' + key + '"=%s')
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('"' + key + '"!=%s')
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('"' + key + '" in %s')
        self.query_values.append(value)

    def commit(self):
        fields = list(self.update.keys()) if not self.fields else self.fields
        db_name = self.name.split(".")
        table_name = ".".join(db_name[1:]) if len(db_name) > 1 else db_name[0]
        if table_name in self.db.tables:
            tags = self.db.tables[table_name].get("tags")
        else:
            tags = None

        json_body = {"measurement": table_name, "tags": {}, "time": None, "fields": {}}
        for field in fields:
            value = escape_param(self.update[field]) if isinstance(self.update[field], datetime.date) else self.update[field]
            if field == "time":
                json_body["time"] = value
            elif tags:
                if field in tags:
                    json_body["tags"][field] = value
                else:
                    json_body["fields"][field] = value
            else:
                if isinstance(self.update[field], (str, datetime.date)) or field in self.primary_keys:
                    json_body["tags"][field] = value
                else:
                    json_body["fields"][field] = value
        if not json_body["time"]:
            json_body["time"] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        connection = self.db.ensure_connection()
        try:
            return connection.write_points([json_body])
        finally:
            self.db.release_connection()

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(self.query[i], self.query_values[i]) for i in range(len(self.query))]),
            human_repr_object(self.diff_data))


class InfluxDBDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(InfluxDBDeleteBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []
        self.sql = None

    def filter_gt(self, key, value):
        self.query.append('"' + key + '">%s')
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('"' + key + '">=%s')
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('"' + key + '"<%s')
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('"' + key + '"<=%s')
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('"' + key + '"=%s')
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('"' + key + '"!=%s')
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('"' + key + '" in %s')
        self.query_values.append(value)

    def commit(self):
        db_name = self.name.split(".")
        db_name = ('"%s"' % ".".join(db_name[1:])) if len(db_name) > 1 else ('"' + db_name[0] + '"')
        query, query_values = [], []
        for i in range(len(self.query)):
            if self.query[i][-6:] == " in %s":
                in_querys = []
                for qv in self.query_values[i]:
                    in_querys.append(self.query[i][:-6] + "=%s")
                    query_values.append(qv)
                query.append("(" + " OR ".join(in_querys) + ")")
            else:
                query.append(self.query[i])
                query_values.append(self.query_values[i])

        sql = "DELETE FROM %s WHERE %s" % (db_name, " AND ".join(self.query))
        connection = self.db.ensure_connection()
        try:
            return connection.query(sql % escape_args(query_values))
        finally:
            self.db.release_connection()
            self.sql = (sql, query_values)

    def verbose(self):
        if isinstance(self.sql, tuple):
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class InfluxDBFactory(DatabaseFactory):
    def create(self):
        try:
            from influxdb import InfluxDBClient
        except ImportError:
            raise ImportError("influxdb>=5.3.1 is required")
        return InfluxDBClient(**self.config)

    def ping(self, driver):
        driver.ping()
        return True

    def close(self, driver):
        driver.close()


class InfluxDB(DataBase):
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 8086,
        "username": "root",
        "password": "root",
        "database": "",
        "virtual_views": [
            # {
            #     "name": "",
            #     "name_match": "",
            #     "query": "",
            #     "args": [],
            # }
        ],
    }

    def __init__(self, manager, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.db_name = all_config["db"] if "db" in all_config else all_config["name"]
        self.virtual_tables = all_config.pop("virtual_views") if "virtual_views" in all_config else []

        super(InfluxDB, self).__init__(manager, all_config)

        self.connection_key = None
        self.connection = None

    def ensure_connection(self):
        if self.connection:
            return self.connection.raw()
        if not self.connection_key:
            self.connection_key = self.get_key(self.config)
            if not self.manager.has(self.connection_key):
                self.manager.register(self.connection_key, InfluxDBFactory(self.connection_key, self.config))
        self.connection = self.manager.acquire(self.connection_key)
        return self.connection.raw()

    def release_connection(self):
        if not self.connection:
            return
        self.manager.release(self.connection_key, self.connection)
        self.connection = None

    def query(self, name, primary_keys=None, fields=()):
        return InfluxDBQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return InfluxDBInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return InfluxDBUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return InfluxDBDeleteBuilder(self, name, primary_keys)

    def close(self):
        if not self.connection:
            return
        self.connection.raw().close()
        self.connection = None

    def verbose(self):
        return "%s<%s>" % (self.name, self.db_name)