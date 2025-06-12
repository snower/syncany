# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import os
import re
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase, DatabaseFactory


def format_placeholder_args(sql, args):
    if not args:
        return sql, args
    placeholder_index, placeholders, values = 1, [], []
    for arg in args:
        if isinstance(arg, (tuple, set, list)):
            sub_placeholders = []
            for v in arg:
                sub_placeholders.append(":%d" % placeholder_index)
                values.append(v)
                placeholder_index += 1
            placeholders.append("(" + ",".join(sub_placeholders) + ")")
        else:
            placeholders.append(":%d" % placeholder_index)
            values.append(arg)
            placeholder_index += 1
    return (sql % tuple(placeholders)), values


class OracleQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(OracleQueryBuilder, self).__init__(*args, **kwargs)

        self.query_values = []
        self.sql = None

        db_name = self.name.split('.')
        if len(db_name) > 1:
            self.table_name = '.'.join(db_name[1:])
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

    def filter_cursor(self, last_data, offset, count, primary_orders=None):
        if primary_orders and last_data and all([primary_key in last_data for primary_key in self.primary_keys]):
            for primary_key in self.primary_keys:
                if primary_key in primary_orders and primary_orders[primary_key] < 0:
                    self.query.append('"' + primary_key + '"<%s')
                else:
                    self.query.append('"' + primary_key + '">%s')
                self.query_values.append(last_data[primary_key])
        else:
            self.limit = (offset, count)

    def order_by(self, key, direct=1):
        self.orders.append(('"' + key + ('" ASC' if direct > 0 else '" DESC')))

    def format_table(self):
        for virtual_table in self.db.virtual_tables:
            if virtual_table.get('name_match'):
                name_match = re.compile(virtual_table.get('name_match'))
                if not name_match.match(self.table_name):
                    continue
                if isinstance(virtual_table['query'], list):
                    virtual_table['query'] = ' '.join(virtual_table['query'])
                sql = virtual_table['query'].replace('"%s"' % virtual_table['name'], '"%s"' % self.table_name)
            elif virtual_table['name'] != self.table_name:
                continue
            else:
                if isinstance(virtual_table['query'], list):
                    virtual_table['query'] = ' '.join(virtual_table['query'])
                sql = virtual_table['query']
            return '(%s) "virtual_%s"' % (sql, self.table_name), virtual_table.get('args', []), True
        return ('"%s"."%s"' % (self.db.db_name, self.table_name)), [], False

    def format_query(self, db_name, virtual_args):
        if not virtual_args:
            return db_name, (' AND '.join(self.query) if self.query else ''), self.query_values

        query, query_values, virtual_query, virtual_values = [], [], {}, []
        for arg in virtual_args:
            if isinstance(arg, str):
                virtual_q = '"' + arg[0] + '"=%s'
            else:
                if arg[1] == '==':
                    virtual_q = '"' + arg[0] + '"=%s'
                elif arg[1] == 'in':
                    virtual_q = '"' + arg[0] + '" ' + arg[1] + ' %s'
                else:
                    virtual_q = '"' + arg[0] + '"' + arg[1] + '%s'
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
            query.append(self.query[i])
            query_values.append(self.query_values[i])
        return db_name, (' AND '.join(query) if query else ''), (virtual_values + query_values)

    def commit(self):
        db_name, virtual_args, is_virtual = self.format_table()
        db_name, query, query_values = self.format_query(db_name, virtual_args)

        if self.fields and not is_virtual:
            fields = []
            for field in self.fields:
                fields.append('"' + field + '"')
            fields = ', '.join(fields)
        else:
            fields = '*'

        where = (' WHERE ' + query) if query else ''
        order_by = (' ORDER BY ' + ','.join(self.orders)) if self.orders else ''
        if self.limit:
            sql = ('SELECT %s FROM (SELECT "_t_".*, ROWNUM as "_rn_" FROM (SELECT %s FROM %s%s%s) "_t_" WHERE ROWNUM <= %s) WHERE "_rn_" > %s'
                        % (fields, fields, db_name, where, order_by, self.limit[0] + self.limit[1], self.limit[0]))
        else:
            sql = 'SELECT %s FROM %s%s%s' % (fields, db_name, where, order_by)
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(*format_placeholder_args(sql, query_values))
            datas = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            datas = [dict(zip(columns, row)) for row in datas]
        finally:
            cursor.close()
            connection.commit()
            self.db.release_connection()
            self.sql = (sql, query_values)
        return datas if isinstance(datas, list) else list(datas)

    def verbose(self):
        if isinstance(self.sql, tuple):
            if '\n' in self.sql[0]:
                return 'sql: \n%s\nargs: %s' % (self.sql[0], human_repr_object(self.sql[1]))
            return 'sql: %s\nargs: %s' % (self.sql[0], human_repr_object(self.sql[1]))
        return 'sql: %s' % self.sql


class OracleInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(OracleInsertBuilder, self).__init__(*args, **kwargs)

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
            datas.append(tuple((data[field] for field in fields)))

        db_name = self.name.split('.')
        db_name = ('"%s"."%s"' % (self.db.db_name, '.'.join(db_name[1:]))) if len(db_name) > 1 else ('"' + db_name[0] + '"')
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (db_name, ','.join(['"' + field + '"' for field in fields]), ','.join([':%d' % (i + 1) for i in range(len(fields))]))
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
            return 'sql: %s\nargs(%d): \n%s' % (self.sql[0], len(self.sql[1]), human_repr_object(self.sql[1]))
        return 'sql: %s' % self.sql


class OracleUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(OracleUpdateBuilder, self).__init__(*args, **kwargs)

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
        values, update = [], []
        for key, value in self.update.items():
            if self.diff_data and key not in self.diff_data:
                continue
            update.append('"' + key + '"=%s')
            values.append(value)
        values += self.query_values

        db_name = self.name.split('.')
        db_name = ('"%s"."%s"' % (self.db.db_name, '.'.join(db_name[1:]))) if len(db_name) > 1 else ('"' + db_name[0] + '"')
        sql = 'UPDATE %s SET %s WHERE %s' % (db_name, ','.join(update), ' AND '.join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(*format_placeholder_args(sql, values))
        finally:
            cursor.close()
            connection.commit()
            self.db.release_connection()
            self.sql = (sql, values)
        return cursor

    def verbose(self):
        if isinstance(self.sql, tuple):
            return 'sql: %s\nargs: %s' % (self.sql[0], human_repr_object(self.sql[1]))
        return 'sql: %s' % self.sql


class OracleDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(OracleDeleteBuilder, self).__init__(*args, **kwargs)

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
        db_name = self.name.split('.')
        db_name = ('"%s"."%s"' % (self.db.db_name, '.'.join(db_name[1:]))) if len(db_name) > 1 else ('"' + db_name[0] + '"')
        if not self.query and self.db.delete_all_truncate_table:
            sql = 'TRUNCATE TABLE %s' % db_name
        else:
            sql = 'DELETE FROM %s WHERE %s' % (db_name, ' AND '.join(self.query))
        connection = self.db.ensure_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(*format_placeholder_args(sql, self.query_values))
        finally:
            cursor.close()
            connection.commit()
            self.db.release_connection()
            self.sql = (sql, self.query_values)
        return cursor

    def verbose(self):
        if isinstance(self.sql, tuple):
            return 'sql: %s\nargs: %s' % (self.sql[0], human_repr_object(self.sql[1]))
        return 'sql: %s' % self.sql


class OracleDBFactory(DatabaseFactory):
    oracle_instant_client_home = None

    def create(self):
        try:
            import oracledb
        except ImportError:
            raise ImportError('oracledb>=3.1.1 is required')
        oracle_instant_client_home = self.config.pop("init_oracle_client", None)
        if not oracle_instant_client_home:
            oracle_instant_client_home = os.environ.get('ORACLE_INSTANTCLIENT_HOME')
        if oracle_instant_client_home and self.__class__.oracle_instant_client_home != oracle_instant_client_home:
            oracledb.init_oracle_client(oracle_instant_client_home)
            self.__class__.oracle_instant_client_home = oracle_instant_client_home
        return oracledb.connect(**self.config)

    def ping(self, driver):
        driver.instance.ping()
        return True

    def close(self, driver):
        driver.instance.close()


class OracleDB(DataBase):
    DEFAULT_CONFIG = {
        'host': '127.0.0.1',
        'port': 1521,
        'user': 'system',
        'password': '',
        'virtual_views': [
            # {
            #     'name': '',
            #     'name_match': '',
            #     'query': '',
            #     'args': [],
            # }
        ],
    }

    def __init__(self, manager, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        if "schema" in all_config:
            self.db_name = all_config.pop("schema")
        else:
            self.db_name = all_config['user'] if 'user' in all_config else all_config['name']
        self.virtual_tables = all_config.pop('virtual_views') if 'virtual_views' in all_config else []
        self.delete_all_truncate_table = all_config.pop('delete_all_truncate_table') \
            if 'delete_all_truncate_table' in all_config else False

        super(OracleDB, self).__init__(manager, all_config)

    def build_factory(self):
        return OracleDBFactory(self.get_config_key(), self.config)

    def ensure_connection(self):
        return self.acquire_driver().raw()

    def release_connection(self):
        self.release_driver()

    def query(self, name, primary_keys=None, fields=()):
        return OracleQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return OracleInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return OracleUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return OracleDeleteBuilder(self, name, primary_keys)

    def config_loader(self, loader):
        super(OracleDB, self).config_outputer(loader)
        if hasattr(loader, "join_batch"):
            loader.join_batch = 1000

    def config_outputer(self, outputer):
        super(OracleDB, self).config_outputer(outputer)
        if hasattr(outputer, "join_batch"):
            outputer.join_batch = 1000

    def restrict_filter_in_max_value_count(self):
        return 1000

    def verbose(self):
        return '%s<%s>' % (self.name, self.db_name)