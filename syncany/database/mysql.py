# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import re
try:
    import pymysql
    from pymysql.cursors import DictCursor
except ImportError:
    pymysql = None

from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

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
        key = self.map_virtual_fields(self.table_name, key)
        self.query.append('`' + key + "`>%s")
        self.query_values.append(value)

    def filter_gte(self, key, value):
        key = self.map_virtual_fields(self.table_name, key)
        self.query.append('`' + key + "`>=%s")
        self.query_values.append(value)

    def filter_lt(self, key, value):
        key = self.map_virtual_fields(self.table_name, key)
        self.query.append('`' + key + "`<%s")
        self.query_values.append(value)

    def filter_lte(self, key, value):
        key = self.map_virtual_fields(self.table_name, key)
        self.query.append('`' + key + "`<=%s")
        self.query_values.append(value)

    def filter_eq(self, key, value):
        key = self.map_virtual_fields(self.table_name, key)
        self.query.append('`' + key + "`=%s")
        self.query_values.append(value)

    def filter_ne(self, key, value):
        key = self.map_virtual_fields(self.table_name, key)
        self.query.append('`' + key + "`!=%s")
        self.query_values.append(value)

    def filter_in(self, key, value):
        key = self.map_virtual_fields(self.table_name, key)
        self.query.append('`' + key + "` in %s")
        self.query_values.append(value)

    def filter_limit(self, count, start=None):
        if start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def order_by(self, key, direct=1):
        self.orders.append(('`' + key + ("` ASC" if direct else "` DESC")))

    def map_virtual_fields(self, table_name, field):
        if table_name in self.db.tables:
            virtual_fields = self.db.tables[table_name].get("virtual_fields", {})
            if field in virtual_fields:
                return virtual_fields[field]
        return field

    def format_table(self):
        for virtual_table in self.db.virtual_tables:
            if virtual_table.get("name_match"):
                name_match = re.compile(virtual_table.get("name_match"))
                if not name_match.match(self.table_name):
                    continue
                if isinstance(virtual_table["sql"], list):
                    virtual_table["sql"] = " ".join(virtual_table["sql"])
                sql = virtual_table['sql'].replace('`%s`' % virtual_table["name"], '`%s`' % self.table_name)
            elif virtual_table["name"] != self.table_name:
                continue
            else:
                if isinstance(virtual_table["sql"], list):
                    virtual_table["sql"] = " ".join(virtual_table["sql"])
                sql = virtual_table['sql']
            return '(%s) `virtual_%s`' % (sql, self.table_name), virtual_table.get("args", [])
        return ("`%s`.`%s`" % (self.db.db_name, self.table_name)), []

    def format_query(self, db_name, virtual_args):
        if not virtual_args:
            return db_name, (" AND ".join(self.query) if self.query else ""), self.query_values

        query, query_values, virtual_query, virtual_values = [], [], {}, []
        for arg in virtual_args:
            if isinstance(arg, str):
                virtual_q = "`" + arg[0] + "`=%s"
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
        db_name, virtual_args = self.format_table()
        db_name, query, query_values = self.format_query(db_name, virtual_args)

        if self.fields:
            fields = []
            for field in self.fields:
                virtual_field = self.map_virtual_fields(self.table_name, field)
                if virtual_field == field:
                    fields.append('`' + field + '`')
                else:
                    fields.append('%s as `%s`' % (virtual_field, field))
            fields = ", ".join(fields)
        else:
            fields = "*"

        where = (" WHERE" + query) if query else ""
        order_by = (" ORDER BY " + ",".join(self.orders)) if self.orders else ""
        limit = (" LIMIT %s%s" % (("%s," % self.limit[0]) if self.limit[0] else "", self.limit[1])) if self.limit else ""
        self.sql = "SELECT %s FROM %s%s%s%s" % (fields, db_name, where, order_by, limit)
        connection = self.db.ensure_connection()
        cursor = connection.cursor(DictCursor)
        try:
            cursor.execute(self.sql, query_values)
            datas = cursor.fetchall()
        finally:
            cursor.close()
            if connection.autocommit_mode == False:
                connection.commit()
        return datas

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
        self.sql = "INSERT INTO %s (%s) VALUES (%s)" % (db_name, ",".join(['`' + field + '`' for field in fields]), ",".join(["%s" for _ in fields]))
        connection = self.db.ensure_connection()
        cursor = connection.cursor(DictCursor)
        try:
            cursor.executemany(self.sql, datas)
        finally:
            cursor.close()
            connection.commit()
        return cursor

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
        self.sql = "UPDATE %s SET %s WHERE %s" % (db_name, ",".join(update), " AND ".join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor(DictCursor)
        try:
            cursor.execute(self.sql, values)
        finally:
            cursor.close()
            connection.commit()
        return cursor

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
        self.sql = "DELETE FROM %s WHERE %s" % (db_name, " AND ".join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor(DictCursor)
        try:
            cursor.execute(self.sql, self.query_values)
        finally:
            cursor.close()
            connection.commit()
        return cursor

class MysqlDB(DataBase):
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "passwd": "",
        "db": "",
        "charset": "utf8mb4",
        "tables": [
            # {
            #     "name": "",
            #     "virtual_fields": {}
            # }
        ],
        "virtual_tables": [
            # {
            #     "name": "",
            #     "name_match": "",
            #     "sql": "",
            #     "args": [],
            #     "schema": {
            #     }
            # }
        ],
    }

    def __init__(self, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.db_name = all_config["db"] if "db" in all_config else all_config["name"]
        self.tables = {table["name"]: table for table in all_config.pop("tables")} if "tables" in all_config else {}
        self.virtual_tables = all_config.pop("virtual_tables") if "virtual_tables" in all_config else []

        super(MysqlDB, self).__init__(all_config)

        self.connection = None

    def ensure_connection(self):
        if not self.connection:
            if pymysql is None:
                raise ImportError("PyMySQL>=0.8.1 is required")

            self.connection = pymysql.Connection(**self.config)
        return self.connection

    def query(self, name, primary_keys=None, fields=()):
        return MysqlQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return MysqlInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return MysqlUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return MysqlDeleteBuilder(self, name, primary_keys)

    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None