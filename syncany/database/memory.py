# -*- coding: utf-8 -*-
# 2020/7/6
# create by: snower

import time
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, CacheBuilder, DataBase, DatabaseFactory


class MemoryQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryQueryBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def filter_cursor(self, last_data, offset, count):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        if not self.query:
            datas = self.db.memory_databases.get(self.name, [])
            if self.limit:
                datas = datas[self.limit[0]: self.limit[1]]
        else:
            index, datas = 0, []
            for data in self.db.memory_databases.get(self.name, []):
                if self.limit and (index < self.limit[0] or index > self.limit[1]):
                    continue
    
                succed = True
                for (key, exp), (value, cmp) in self.query.items():
                    if key not in data:
                        succed = False
                        break
                    if not cmp(data[key], value):
                        succed = False
                        break
    
                if succed:
                    datas.append(data)
                    index += 1

        if self.orders:
            datas = sorted(datas, key=lambda x: x.get(self.orders[0][0]), reverse=True if self.orders[0][1] < 0 else False)
        return datas

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            self.limit,
            self.orders)


class MemoryInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        datas = self.db.memory_databases.get(self.name, [])
        datas.extend(self.datas)
        self.db.memory_databases[self.name] = datas

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)


class MemoryUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryUpdateBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def commit(self):
        datas = []
        for data in self.db.memory_databases.get(self.name, []):
            succed = True
            for (key, exp), (value, cmp) in self.query.items():
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if succed:
                datas.append(self.update)
            else:
                datas.append(data)

        self.db.memory_databases[self.name] = datas
        return datas

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()]),
            human_repr_object(self.diff_data))


class MemoryDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryDeleteBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def commit(self):
        datas = []
        for data in self.db.memory_databases.get(self.name, []):
            succed = True
            for (key, exp), (value, cmp) in self.query.items():
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if not succed:
                datas.append(data)

        self.db.memory_databases[self.name] = datas
        return datas

    def verbose(self):
        return "filters: %s" % human_repr_object([(key, exp, value) for (key, exp), (value, cmp) in self.query.items()])


class MemoryCacheBuilder(CacheBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryCacheBuilder, self).__init__(*args, **kwargs)

        self.caches = self.db.memory_databases.get("@caches::" + self.prefix_key, {})

    def get(self, key):
        if key not in self.caches:
            return None
        value = self.caches[key]
        if time.time() > value["expried_time"]:
            self.caches.pop(key, None)
            return None
        return value["value"]

    def set(self, key, value, exprie_seconds=86400):
        self.caches[key] = {
            "value": value,
            "expried_time": time.time() + exprie_seconds
        }

    def delete(self, key):
        if key not in self.caches:
            return False
        self.caches.pop(key, None)
        return True


class MemoryDBDriver(dict):
    pass


class MemoryDBFactory(DatabaseFactory):
    def create(self):
        return MemoryDBDriver()

    def ping(self, driver):
        return True

    def close(self, driver):
        pass


class MemoryDB(DataBase):
    def __init__(self, manager, config):
        super(MemoryDB, self).__init__(manager, dict(**config))

        self.memory_databases = None

    def ensure_memory_databases(self):
        if self.memory_databases is not None:
            return
        key = self.get_key(self.config)
        if not self.manager.has(key):
            self.manager.register(key, MemoryDBFactory(key, self.config))
        db = self.manager.acquire(key)
        self.memory_databases = db.raw()
        self.manager.release(key, db)

    def query(self, name, primary_keys=None, fields=()):
        self.ensure_memory_databases()
        return MemoryQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        self.ensure_memory_databases()
        return MemoryInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        self.ensure_memory_databases()
        return MemoryUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        self.ensure_memory_databases()
        return MemoryDeleteBuilder(self, name, primary_keys)

    def cache(self, name, prefix_key, config=None):
        self.ensure_memory_databases()
        return MemoryCacheBuilder(self, name, prefix_key, config)