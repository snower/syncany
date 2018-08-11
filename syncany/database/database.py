# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

class QueryBuilder(object):
    def __init__(self, db, name, fields):
        self.db = db
        self.name = name
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
    def __init__(self, db, name, datas):
        self.db = db
        self.name = name
        self.datas = datas

    def commit(self):
        raise NotImplementedError()

class UpdateBuilder(object):
    def __init__(self, db, name, update):
        self.db = db
        self.name = name
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
    def __init__(self, db, name):
        self.db = db
        self.name = name
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

    def query(self, name, *fields):
        return QueryBuilder(self, name, fields)

    def insert(self, name, datas):
        return InsertBuilder(self, name, datas)

    def update(self, name, **update):
        return UpdateBuilder(self, name, update)

    def delete(self, name):
        return InsertBuilder(self, name)

    def close(self):
        pass

    def get_default_loader(self):
        return "db_loader"

    def get_default_outputer(self):
        return "db_update_insert_outputer"