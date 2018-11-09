# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

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
        if start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def order_by(self, key, direct=1):
        self.orders.append(('`' + key + ("` ASC" if direct else "` DESC")))

    def commit(self):
        db_name = self.name.split(".")
        db_name = ("`%s`.`%s`" % (self.db.db_name, ".".join(db_name[1:]))) if len(db_name) > 1 else ('`' + db_name[0] + '`')
        fields = ", ".join(['`' + field + '`' for field in self.fields]) if self.fields else "*"

        where = (" WHERE" + " AND ".join(self.query)) if self.query else ""
        order_by = (" ORDER BY " + ",".join(self.orders)) if self.orders else ""
        limit = (" LIMIT %s%s" % (("%s," % self.limit[0]) if self.limit[0] else "", self.limit[1])) if self.limit else ""
        self.sql = "SELECT %s FROM %s%s%s%s" % (fields, db_name, where, order_by, limit)
        connection = self.db.ensure_connection()
        cursor = connection.cursor(DictCursor)
        try:
            cursor.execute(self.sql, self.query_values)
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
        values = []
        update = []
        for key, value in self.update.items():
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
        "charset": "utf8mb4"
    }

    def __init__(self, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.db_name = all_config["db"] if "db" in all_config else all_config["name"]

        super(MysqlDB, self).__init__(all_config)

        self.connection = None

    def ensure_connection(self):
        if not self.connection:
            if pymysql is None:
                raise ImportError("PyMySQL>=0.8.1 is required")

            self.connection = pymysql.Connection(**self.config)
        return self.connection

    def query(self, name, primary_keys = None, fields = ()):
        return MysqlQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys = None, fields = (), datas = None):
        return MysqlInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys = None, fields = (), update = None):
        return MysqlUpdateBuilder(self, name, primary_keys, fields, update)

    def delete(self, name, primary_keys = None):
        return MysqlDeleteBuilder(self, name, primary_keys)

    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None