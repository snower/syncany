# -*- coding: utf-8 -*-
# 2020/11/27
# create by: snower

import re
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase, DatabaseFactory


class ClickhouseQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(ClickhouseQueryBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []
        self.sql = None

        db_name = self.name.split(".")
        if len(db_name) > 1:
            self.table_name = ".".join(db_name[1:])
        else:
            self.table_name = db_name[0]

    def filter_gt(self, key, value):
        self.query.append('`' + key + "`>%s")
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('`' + key + "`>=%s")
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('`' + key + "`<%s")
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('`' + key + "`<=%s")
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('`' + key + "`=%s")
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('`' + key + "`!=%s")
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('`' + key + "` in %s")
        self.query_values.append(value)

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def filter_cursor(self, last_data, offset, count):
        if len(self.primary_keys) == 1 and self.primary_keys[0] in last_data:
            self.query.append('`' + self.primary_keys[0] + "`>%s")
            self.query_values.append(last_data[self.primary_keys[0]])
        else:
            self.limit = (offset, count)

    def order_by(self, key, direct=1):
        self.orders.append(('`' + key + ("` ASC" if direct else "` DESC")))

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
            return '(%s) `virtual_%s`' % (sql, self.table_name), virtual_table.get("args", []), True
        return ("`%s`.`%s`" % (self.db.db_name, self.table_name)), [], False

    def format_query(self, db_name, virtual_args):
        if not virtual_args:
            return db_name, (" AND ".join(self.query) if self.query else ""), self.query_values

        query, query_values, virtual_query, virtual_values = [], [], {}, []
        for arg in virtual_args:
            if isinstance(arg, str):
                virtual_q = "`" + arg[0] + "`=%s"
            else:
                if arg[1] == "in":
                    virtual_q = "`" + arg[0] + "` " + arg[1] + " %s"
                else:
                    virtual_q = "`" + arg[0] + "`" + arg[1] + "%s"
            for i in range(len(self.query)):
                if self.query[i] == virtual_q:
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
            cursor.execute(sql % self.db.escape_args(query_values))
            datas = cursor.fetchall()
            names = [c.name for c in cursor.description]
            datas = [dict(zip(names, data)) for data in datas]
        finally:
            cursor.close()
            self.db.release_connection()
            self.sql = (sql, query_values)
        return datas

    def verbose(self):
        if isinstance(self.sql, tuple):
            if "\n" in self.sql[0]:
                return "sql: \n%s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class ClickhouseInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(ClickhouseInsertBuilder, self).__init__(*args, **kwargs)

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
        db_name = ("`%s`.`%s`" % (self.db.db_name, ".".join(db_name[1:]))) if len(db_name) > 1 else ('`' + db_name[0] + '`')
        sql = "INSERT INTO %s (%s) VALUES " % (db_name, ",".join(['`' + field + '`' for field in fields]))
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.executemany(sql, datas)
        finally:
            cursor.close()
            self.db.release_connection()
            self.sql = (sql, datas)
        return cursor

    def verbose(self):
        if isinstance(self.sql, tuple):
            args = ",\n    ".join([human_repr_object(value) for value in self.sql[1]])
            return "sql: %s\nargs(%d): \n[\n    %s\n]" % (self.sql[0], len(self.sql[1]), args)
        return "sql: %s" % self.sql


class ClickhouseUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(ClickhouseUpdateBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []
        self.sql = None

    def filter_gt(self, key, value):
        self.query.append('`' + key + "`>%s")
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('`' + key + "`>=%s")
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('`' + key + "`<%s")
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('`' + key + "`<=%s")
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('`' + key + "`=%s")
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('`' + key + "`!=%s")
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('`' + key + "` in %s")
        self.query_values.append(value)

    def commit(self):
        values, update = [], []
        for key, value in self.update.items():
            if self.diff_data and key not in self.diff_data:
                continue
            update.append('`' + key + "`=%s")
            values.append(value)
        values += self.query_values

        db_name = self.name.split(".")
        db_name = ("`%s`.`%s`" % (self.db.db_name, ".".join(db_name[1:]))) if len(db_name) > 1 else ('`' + db_name[0] + '`')
        sql = "ALTER TABLE %s UPDATE %s WHERE %s" % (db_name, ",".join(update), " AND ".join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql % self.db.escape_args(values))
        finally:
            cursor.close()
            self.db.release_connection()
            self.sql = (sql, values)
        return cursor

    def verbose(self):
        if isinstance(self.sql, tuple):
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class ClickhouseDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(ClickhouseDeleteBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []
        self.sql = None

    def filter_gt(self, key, value):
        self.query.append('`' + key + "`>%s")
        self.query_values.append(value)

    def filter_gte(self, key, value):
        self.query.append('`' + key + "`>=%s")
        self.query_values.append(value)

    def filter_lt(self, key, value):
        self.query.append('`' + key + "`<%s")
        self.query_values.append(value)

    def filter_lte(self, key, value):
        self.query.append('`' + key + "`<=%s")
        self.query_values.append(value)

    def filter_eq(self, key, value):
        self.query.append('`' + key + "`=%s")
        self.query_values.append(value)

    def filter_ne(self, key, value):
        self.query.append('`' + key + "`!=%s")
        self.query_values.append(value)

    def filter_in(self, key, value):
        self.query.append('`' + key + "` in %s")
        self.query_values.append(value)

    def commit(self):
        db_name = self.name.split(".")
        db_name = ("`%s`.`%s`" % (self.db.db_name, ".".join(db_name[1:]))) if len(db_name) > 1 else ('`' + db_name[0] + '`')
        sql = "ALTER TABLE %s DELETE WHERE %s" % (db_name, " AND ".join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql % self.db.escape_args(self.query_values))
        finally:
            cursor.close()
            self.db.release_connection()
            self.sql = (sql, self.query_values)
        return cursor

    def verbose(self):
        if isinstance(self.sql, tuple):
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class ClickhouseDBFactory(DatabaseFactory):
    def create(self):
        try:
            import clickhouse_driver
            from clickhouse_driver.util.escape import escape_param
        except ImportError:
            raise ImportError("clickhouse_driver>=0.1.5 is required")
        return clickhouse_driver.connect(**self.config)

    def ping(self, driver):
        return driver.ping()

    def close(self, driver):
        driver.close()


class ClickhouseDB(DataBase):
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 9000,
        "user": "root",
        "password": "",
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

        self.db_name = all_config["database"] if "database" in all_config else all_config["name"]
        self.virtual_tables = all_config.pop("virtual_views") if "virtual_views" in all_config else []

        super(ClickhouseDB, self).__init__(manager, all_config)

        self.connection_key = None
        self.connection = None
        self.escape_param = lambda arg: arg

    def ensure_connection(self):
        if self.connection:
            return self.connection.raw()
        if not self.connection_key:
            self.connection_key = self.get_key(self.config)
            if not self.manager.has(self.connection_key):
                self.manager.register(self.connection_key, ClickhouseDBFactory(self.connection_key, self.config))

            try:
                from clickhouse_driver.util.escape import escape_param
            except ImportError:
                raise ImportError("clickhouse_driver>=0.1.5 is required")
            self.escape_param = escape_param
        self.connection = self.manager.acquire(self.connection_key)
        return self.connection.raw()

    def release_connection(self):
        if not self.connection:
            return
        self.manager.release(self.connection_key, self.connection)
        self.connection = None

    def query(self, name, primary_keys=None, fields=()):
        return ClickhouseQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return ClickhouseInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return ClickhouseUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return ClickhouseDeleteBuilder(self, name, primary_keys)

    def close(self):
        if not self.connection:
            return
        self.connection.raw().close()
        self.connection = None

    def verbose(self):
        return "%s<%s>" % (self.name, self.db_name)

    def escape_args(self, args):
        if isinstance(args, set):
            return tuple(self.escape_param(list(arg)) for arg in args)
        elif isinstance(args, (list, tuple)):
            return tuple(self.escape_param(arg) for arg in args)
        elif isinstance(args, dict):
            return {key: self.escape_param(val) for (key, val) in args.items()}
        else:
            return self.escape_param(args)