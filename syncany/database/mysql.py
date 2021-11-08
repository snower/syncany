# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import re
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase, DatabaseFactory


class MysqlQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(MysqlQueryBuilder, self).__init__(*args, **kwargs)

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
        cursor = connection.cursor(self.db.DictCursor)
        try:
            cursor.execute(sql, query_values)
            datas = cursor.fetchall()
        finally:
            cursor.close()
            if connection.autocommit_mode == False:
                connection.commit()
            self.db.release_connection()
            self.sql = (sql, query_values)
        return datas

    def verbose(self):
        if isinstance(self.sql, tuple):
            if "\n" in self.sql[0]:
                return "sql: \n%s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
            return "sql: %s\nargs: %s" % (self.sql[0], human_repr_object(self.sql[1]))
        return "sql: %s" % self.sql


class MysqlInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(MysqlInsertBuilder, self).__init__(*args, **kwargs)

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
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (db_name, ",".join(['`' + field + '`' for field in fields]), ",".join(["%s" for _ in fields]))
        connection = self.db.ensure_connection()
        cursor = connection.cursor(self.db.DictCursor)
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
            args = ",\n    ".join([human_repr_object(value) for value in self.sql[1]])
            return "sql: %s\nargs(%d): \n[\n    %s\n]" % (self.sql[0], len(self.sql[1]), args)
        return "sql: %s" % self.sql


class MysqlUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(MysqlUpdateBuilder, self).__init__(*args, **kwargs)

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
        sql = "UPDATE %s SET %s WHERE %s" % (db_name, ",".join(update), " AND ".join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor(self.db.DictCursor)
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


class MysqlDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(MysqlDeleteBuilder, self).__init__(*args, **kwargs)

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
        sql = "DELETE FROM %s WHERE %s" % (db_name, " AND ".join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor(self.db.DictCursor)
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


class MysqlDBFactory(DatabaseFactory):
    def create(self):
        try:
            import pymysql
        except ImportError:
            raise ImportError("PyMySQL>=0.8.1 is required")
        return pymysql.Connection(**self.config)

    def ping(self, driver):
        driver.ping()
        return True

    def close(self, driver):
        driver.close()


class MysqlDB(DataBase):
    DictCursor = None
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "passwd": "",
        "db": "",
        "charset": "utf8mb4",
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

        super(MysqlDB, self).__init__(manager, all_config)

        self.connection_key = None
        self.connection = None

    def ensure_connection(self):
        if self.connection:
            return self.connection.raw()
        if not self.connection_key:
            self.connection_key = self.get_key(self.config)
            if not self.manager.has(self.connection_key):
                self.manager.register(self.connection_key, MysqlDBFactory(self.connection_key, self.config))

            try:
                from pymysql.cursors import DictCursor
            except ImportError:
                raise ImportError("PyMySQL>=0.8.1 is required")
            self.DictCursor = DictCursor
        self.connection = self.manager.acquire(self.connection_key)
        return self.connection.raw()

    def release_connection(self):
        if not self.connection:
            return
        self.manager.release(self.connection_key, self.connection)
        self.connection = None

    def query(self, name, primary_keys=None, fields=()):
        return MysqlQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return MysqlInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return MysqlUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return MysqlDeleteBuilder(self, name, primary_keys)

    def close(self):
        if not self.connection:
            return
        self.connection.raw().close()
        self.connection = None

    def verbose(self):
        return "%s<%s>" % (self.name, self.db_name)