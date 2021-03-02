# -*- coding: utf-8 -*-
# 2020/7/6
# create by: snower

from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

MEMORY_DATABASES = {
}


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
        if start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def filter_cursor(self, last_data, offset, count):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        if not self.query:
            datas = MEMORY_DATABASES.get(self.name, [])
            if self.limit:
                datas = datas[self.limit[0]: self.limit[1]]
        else:
            index, datas = 0, []
            for data in MEMORY_DATABASES.get(self.name, []):
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

class MemoryInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        datas = MEMORY_DATABASES.get(self.name, [])
        datas.extend(self.datas)
        MEMORY_DATABASES[self.name] = datas

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
        for data in MEMORY_DATABASES.get(self.name, []):
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

        MEMORY_DATABASES[self.name] = datas
        return datas

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
        for data in MEMORY_DATABASES.get(self.name, []):
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

        MEMORY_DATABASES[self.name] = datas
        return datas

class MemoryDB(DataBase):
    def __init__(self, config):
        super(MemoryDB, self).__init__(dict(**config))

    def query(self, name, primary_keys=None, fields=()):
        return MemoryQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return MemoryInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return MemoryUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return MemoryDeleteBuilder(self, name, primary_keys)