# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import re
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase, DatabaseFactory


class SqliteQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(SqliteQueryBuilder, self).__init__(*args, **kwargs)

        self.query_values = []
        self.sql = None

        db_name = self.name.split(".")
        if len(db_name) > 1:
            self.table_name = ".".join(db_name[1:])
        else:
            self.table_name = db_name[0]

    def filter_gt(self, key, value):
        self.query.append('`' + key + "`>?")
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('`' + key + "`>=?")
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('`' + key + "`<?")
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('`' + key + "`<=?")
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('`' + key + "`=?")
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('`' + key + "`!=?")
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('`' + key + "` in (" + ("?" * len(value)) + ")")
        self.query_values.extend(value)

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def filter_cursor(self, last_data, offset, count, primary_orders=None):
        if primary_orders and last_data and all([primary_key in last_data for primary_key in self.primary_keys]):
            for primary_key in self.primary_keys:
                if primary_key in primary_orders and primary_orders[primary_key] < 0:
                    self.query.append('`' + primary_key + "`<?")
                else:
                    self.query.append('`' + primary_key + "`>?")
                self.query_values.append(last_data[primary_key])
        else:
            self.limit = (offset, count)

    def order_by(self, key, direct=1):
        self.orders.append(('`' + key + ("` ASC" if direct > 0 else "` DESC")))

    def format_table(self):
        for virtual_table in self.db.virtual_tables:
            if virtual_table.get("name_match"):
                name_match = re.compile(virtual_table.get("name_match"))
                if not name_match.match(self.table_name):
                    continue
                if isinstance(virtual_table["query"], list):
                    virtual_table["query"] = " ".join(virtual_table["query"])
                sql = virtual_table['query'].replace('`%s`' % virtual_table["name"], '`%s`' % self.table_name)
            elif virtual_table["name"] != self.table_name:
                continue
            else:
                if isinstance(virtual_table["query"], list):
                    virtual_table["query"] = " ".join(virtual_table["query"])
                sql = virtual_table['query']
            return '(%s) `virtual_%s`' % (sql.replace("%s", "?"), self.table_name), virtual_table.get("args", []), True
        return ("`%s`" % self.table_name), [], False

    def format_query(self, db_name, virtual_args):
        if not virtual_args:
            return db_name, (" AND ".join(self.query) if self.query else ""), self.query_values

        query, query_values, virtual_query, virtual_values = [], [], {}, []
        for arg in virtual_args:
            if isinstance(arg, str):
                virtual_q = "`" + arg[0] + "`=?"
            else:
                if arg[1] == "==":
                    virtual_q = "`" + arg[0] + "`=?"
                elif arg[1] == "in":
                    virtual_q = "`" + arg[0] + "` " + arg[1] + " ("
                else:
                    virtual_q = "`" + arg[0] + "`" + arg[1] + "?"
            for i in range(len(self.query)):
                if self.query[i].startswith(virtual_q):
                    virtual_query[self.query[i]] = self.query_values[i]
                    if isinstance(arg, str):
                        db_name = db_name.replace('`' + arg + '`', '`' + self.query_values[i] + '`')
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
            query.append(self.query[i])
            query_values.append(self.query_values[i])
        return db_name, (" AND ".join(query) if query else ""), (virtual_values + query_values)

    def commit(self):
        db_name, virtual_args, is_virtual = self.format_table()
        db_name, query, query_values = self.format_query(db_name, virtual_args)

        if self.fields and not is_virtual:
            fields = []
            for field in self.fields:
                fields.append('`' + field + '`')
            fields = ", ".join(fields)
        else:
            fields = "*"

        where = (" WHERE " + query) if query else ""
        order_by = (" ORDER BY " + ",".join(self.orders)) if self.orders else ""
        limit = (" LIMIT %s%s" % (("%s," % self.limit[0]) if self.limit[0] else "", self.limit[1])) if self.limit else ""
        sql = "SELECT %s FROM %s%s%s%s" % (fields, db_name, where, order_by, limit)
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql, query_values)
            datas = cursor.fetchall()
        finally:
            cursor.close()
            connection.commit()
            self.db.release_connection()
            self.sql = (sql, query_values)
        return [dict(data) for data in datas]

    def verbose(self):
        if isinstance(self.sql, tuple):
            if "\n" in self.sql[0]:
                return "sql: \n%s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class SqliteInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(SqliteInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]
        self.sql = None

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
        datas = []
        for data in self.datas:
            datas.append([data[field] for field in fields])

        db_name = self.name.split(".")
        db_name = ("`%s`" % (".".join(db_name[1:]))) if len(db_name) > 1 else ('`' + db_name[0] + '`')
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (db_name, ",".join(['`' + field + '`' for field in fields]), ",".join(["?" for _ in fields]))
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.executemany(sql, datas)
        finally:
            cursor.close()
            connection.commit()
            self.db.release_connection()
            self.sql = (sql, datas)
        return cursor

    def verbose(self):
        if isinstance(self.sql, tuple):
            return "sql: %s\nargs(%d): \n%s" % (self.sql[0], len(self.sql[1]), human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class SqliteUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(SqliteUpdateBuilder, self).__init__(*args, **kwargs)

        self.query_values = []
        self.sql = None

    def filter_gt(self, key, value):
        self.query.append('`' + key + "`>?")
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('`' + key + "`>=?")
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('`' + key + "`<?")
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('`' + key + "`<=?")
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('`' + key + "`=?")
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('`' + key + "`!=?")
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('`' + key + "` in (" + ("?" * len(value)) + ")")
        self.query_values.extend(value)

    def commit(self):
        values, update = [], []
        for key, value in self.update.items():
            if self.diff_data and key not in self.diff_data:
                continue
            update.append('`' + key + "`=?")
            values.append(value)
        values += self.query_values

        db_name = self.name.split(".")
        db_name = ("`%s`" % (".".join(db_name[1:]))) if len(db_name) > 1 else ('`' + db_name[0] + '`')
        sql = "UPDATE %s SET %s WHERE %s" % (db_name, ",".join(update), " AND ".join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql, values)
        finally:
            cursor.close()
            connection.commit()
            self.db.release_connection()
            self.sql = (sql, values)
        return cursor

    def verbose(self):
        if isinstance(self.sql, tuple):
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class SqliteDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(SqliteDeleteBuilder, self).__init__(*args, **kwargs)

        self.query_values = []
        self.sql = None

    def filter_gt(self, key, value):
        self.query.append('`' + key + "`>?")
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('`' + key + "`>=?")
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('`' + key + "`<?")
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('`' + key + "`<=?")
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('`' + key + "`=?")
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('`' + key + "`!=?")
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('`' + key + "` in (" + ("?" * len(value)) + ")")
        self.query_values.extend(value)

    def commit(self):
        db_name = self.name.split(".")
        db_name = ("`%s`" % (".".join(db_name[1:]))) if len(db_name) > 1 else ('`' + db_name[0] + '`')
        sql = "DELETE FROM %s WHERE %s" % (db_name, " AND ".join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql, self.query_values)
        finally:
            cursor.close()
            connection.commit()
            self.db.release_connection()
            self.sql = (sql, self.query_values)
        return cursor

    def verbose(self):
        if isinstance(self.sql, tuple):
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class SqliteDBFactory(DatabaseFactory):
    def create(self):
        try:
            import sqlite3
        except ImportError:
            raise ImportError("sqlite3 is required")
        conn = sqlite3.connect(**self.config)
        conn.row_factory = sqlite3.Row
        return conn

    def ping(self, driver):
        driver.instance.ping()
        return True

    def close(self, driver):
        driver.instance.close()


class SqliteDB(DataBase):
    DEFAULT_CONFIG = {
        "database": ":memory:",
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

        self.virtual_tables = all_config.pop("virtual_views") if "virtual_views" in all_config else []

        super(SqliteDB, self).__init__(manager, all_config)

    def build_factory(self):
        return SqliteDBFactory(self.get_config_key(), self.config)

    def ensure_connection(self):
        return self.acquire_driver().raw()

    def release_connection(self):
        self.release_driver()

    def query(self, name, primary_keys=None, fields=()):
        return SqliteQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return SqliteInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return SqliteUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return SqliteDeleteBuilder(self, name, primary_keys)

    def verbose(self):
        return "%s" % self.name