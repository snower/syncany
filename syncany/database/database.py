# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

class QueryBuilder(object):
    def __init__(self, db, name, primary_keys, fields):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.fields = fields
        self.query = {}
        self.orders = []

    def filter_gt(self, key, value):
        raise NotImplementedError()

    def filter_gte(self, key, value):
        raise NotImplementedError()

    def filter_lt(self, key, value):
        raise NotImplementedError()

    def filter_lte(self, key, value):
        raise NotImplementedError()

    def filter_eq(self, key, value):
        raise NotImplementedError()

    def filter_ne(self, key, value):
        raise NotImplementedError()

    def filter_in(self, key, value):
        raise NotImplementedError()

    def order_by(self, key, direct = 1):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

class InsertBuilder(object):
    def __init__(self, db, name, primary_keys, datas):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.datas = datas

    def commit(self):
        raise NotImplementedError()

class UpdateBuilder(object):
    def __init__(self, db, name, primary_keys, update):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.query = {}
        self.update = update

    def filter_gt(self, key, value):
        raise NotImplementedError()

    def filter_gte(self, key, value):
        raise NotImplementedError()

    def filter_lt(self, key, value):
        raise NotImplementedError()

    def filter_lte(self, key, value):
        raise NotImplementedError()

    def filter_eq(self, key, value):
        raise NotImplementedError()

    def filter_ne(self, key, value):
        raise NotImplementedError()

    def filter_in(self, key, value):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

class DeleteBuilder(object):
    def __init__(self, db, name, primary_keys):
        self.db = db
        self.name = name
        self.primary_keys = primary_keys or []
        self.query = {}

    def filter_gt(self, key, value):
        raise NotImplementedError()

    def filter_gte(self, key, value):
        raise NotImplementedError()

    def filter_lt(self, key, value):
        raise NotImplementedError()

    def filter_lte(self, key, value):
        raise NotImplementedError()

    def filter_eq(self, key, value):
        raise NotImplementedError()

    def filter_ne(self, key, value):
        raise NotImplementedError()

    def filter_in(self, key, value):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()

class DataBase(object):
    def __init__(self, config):
        self.name = config.pop("name")
        self.config = config

    def query(self, name, primary_keys = None, *fields):
        return QueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys = None, datas = None):
        return InsertBuilder(self, name, primary_keys, datas)

    def update(self, name, primary_keys = None, **update):
        return UpdateBuilder(self, name, primary_keys, update)

    def delete(self, name, primary_keys = None):
        return DeleteBuilder(self, name, primary_keys)

    def close(self):
        pass

    def get_default_loader(self):
        return "db_loader"

    def get_default_outputer(self):
        return "db_update_insert_outputer"