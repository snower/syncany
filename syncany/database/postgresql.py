# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

try:
    import psycopg2
except ImportError:
    psycopg2 = None

from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

class PostgresqlQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(PostgresqlQueryBuilder, self).__init__(*args, **kwargs)

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
        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
        try:
            cursor.execute(self.sql, self.query_values)
            datas = [data for data in cursor]
        finally:
            cursor.close()
            connection.commit()
        return datas

class PostgresqlInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(PostgresqlInsertBuilder, self).__init__(*args, **kwargs)

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
        cursor = connection.cursor()
        try:
            cursor.executemany(self.sql, datas)
        finally:
            cursor.close()
            connection.commit()
        return cursor

class PostgresqlUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(PostgresqlUpdateBuilder, self).__init__(*args, **kwargs)

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
        cursor = connection.cursor()
        try:
            cursor.execute(self.sql, values)
        finally:
            cursor.close()
            connection.commit()
        return cursor

class PostgresqlDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(PostgresqlDeleteBuilder, self).__init__(*args, **kwargs)

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
        cursor = connection.cursor()
        try:
            cursor.execute(self.sql, self.query_values)
        finally:
            cursor.close()
            connection.commit()
        return cursor

class PostgresqlDB(DataBase):
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 5432,
        "user": "root",
        "password": "",
        "dbname": "",
    }

    def __init__(self, config):
        if "db" in config:
            config["dbname"] = config.pop("db")

        if "passwd" in config:
            config["password"] = config.pop("passwd")

        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.db_name = all_config["dbname"] if "dbname" in all_config else all_config["name"]

        super(PostgresqlDB, self).__init__(all_config)

        self.connection = None

    def ensure_connection(self):
        if not self.connection:
            if psycopg2 is None:
                raise ImportError("psycopg2>=2.7.4 is required")

            self.connection = psycopg2.connect(**self.config)
        return self.connection

    def query(self, name, primary_keys = None, fields = ()):
        return PostgresqlQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys = None, fields = (), datas = None):
        return PostgresqlInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys = None, fields = (), update = None):
        return PostgresqlUpdateBuilder(self, name, primary_keys, fields, update)

    def delete(self, name, primary_keys = None):
        return PostgresqlDeleteBuilder(self, name, primary_keys)

    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None