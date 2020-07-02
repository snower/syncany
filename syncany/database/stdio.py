# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

import sys
import csv
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

class StdioQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(StdioQueryBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        pass

    def filter_gte(self, key, value):
        pass

    def filter_lt(self, key, value):
        pass

    def filter_lte(self, key, value):
        pass

    def filter_eq(self, key, value):
        pass

    def filter_ne(self, key, value):
        pass

    def filter_in(self, key, value):
        pass

    def filter_limit(self, count, start=None):
        pass

    def order_by(self, key, direct=1):
        pass

    def commit(self):
        return []

class StdioInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(StdioInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        writer = csv.writer(sys.stdout, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(self.fields)

        for data in self.datas:
            data = [data[field] for field in self.fields]
            writer.writerow(data)

class StdioUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(StdioUpdateBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        pass

    def filter_gte(self, key, value):
        pass

    def filter_lt(self, key, value):
        pass

    def filter_lte(self, key, value):
        pass

    def filter_eq(self, key, value):
        pass

    def filter_ne(self, key, value):
        pass

    def filter_in(self, key, value):
        pass

    def commit(self):
        return []

class StdioDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(StdioDeleteBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        pass

    def filter_gte(self, key, value):
        pass

    def filter_lt(self, key, value):
        pass

    def filter_lte(self, key, value):
        pass

    def filter_eq(self, key, value):
        pass

    def filter_ne(self, key, value):
        pass

    def filter_in(self, key, value):
        pass

    def commit(self):
        return []

class StdioDB(DataBase):
    def __init__(self, config):
        super(StdioDB, self).__init__(dict(**config))

    def query(self, name, primary_keys=None, fields=()):
        return StdioQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return StdioInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None):
        return StdioUpdateBuilder(self, name, primary_keys, fields, update)

    def delete(self, name, primary_keys=None):
        return StdioDeleteBuilder(self, name, primary_keys)