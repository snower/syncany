# -*- coding: utf-8 -*-
# 2020/7/6
# create by: snower

import time
from ..utils import human_repr_object, sorted_by_keys
from ..taskers.context import TaskerContext
from ..taskers.iterator import TaskerDataIterator
from .database import Cmper, QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, CacheBuilder, DataBase, DatabaseFactory


class MemoryQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryQueryBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query.append((key, '>', value, Cmper.cmp_gt))

    def filter_gte(self, key, value):
        self.query.append((key, ">=", value, Cmper.cmp_gte))

    def filter_lt(self, key, value):
        self.query.append((key, "<", value, Cmper.cmp_lt))

    def filter_lte(self, key, value):
        self.query.append((key, "<=", value, Cmper.cmp_lte))

    def filter_eq(self, key, value):
        self.query.append((key, "==", value, Cmper.cmp_eq))

    def filter_ne(self, key, value):
        self.query.append((key, "!=", value, Cmper.cmp_ne))

    def filter_in(self, key, value):
        try:
            self.query.append((key, "in", set(value) if isinstance(value, list) else value, Cmper.cmp_in))
        except:
            self.query.append((key, "in", value, Cmper.cmp_in))

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, start + count)

    def filter_cursor(self, last_data, offset, count, primary_orders=None):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        tasker_context, iterator, iterator_name, datas = None, None, None, None
        if self.limit and (self.query or self.orders):
            tasker_context = TaskerContext.current()
            if tasker_context:
                iterator_name = "memory::" + self.name
                iterator = tasker_context.get_iterator(iterator_name)
                if iterator and iterator.offset == self.limit[0]:
                    datas, iterator.offset = iterator.datas, self.limit[1]

        if not datas:
            if not self.query:
                datas = self.db.memory_databases.get(self.name, [])[:]
            else:
                datas = []
                for data in self.db.memory_databases.get(self.name, []):
                    succed = True
                    for key, exp, value, cmp in self.query:
                        if key not in data:
                            succed = False
                            break
                        if not cmp(data[key], value):
                            succed = False
                            break
                    if succed:
                        datas.append(data)

            if self.orders:
                datas = sorted_by_keys(datas, keys=[(key, True if direct < 0 else False)
                                                    for key, direct in self.orders] if self.orders else None)
            if tasker_context and self.limit and (self.query or self.orders):
                tasker_context.add_iterator(iterator_name, TaskerDataIterator(datas, self.limit[1]))

        if self.limit:
            datas = datas[self.limit[0]: self.limit[1]]

        if not datas:
            db_keys = self.name.split(".")
            if len(db_keys) != 2 or db_keys[1][:2] != "--":
                return datas
            if len(db_keys[1]) <= 2:
                return [] if self.limit and self.limit[0] > 0 else [{}]
            try:
                count = int(db_keys[1][2:])
                if self.limit:
                    count = max(count - self.limit[0], 0)
                return [{} for _ in range(min(count, self.limit[1]) if self.limit else count)]
            except:
                return datas
        return datas

    def verbose(self):
        return "filters: %s\nlimit: %s\norderBy: %s" % (
            human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query]),
            self.limit,
            self.orders)


class MemoryInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        if ".--" in self.name:
            cache_keys = self.name.split(".")
            if len(cache_keys) == 2 or cache_keys[1][:2] == "--":
                return
        datas = self.db.memory_databases.get(self.name, [])
        datas.extend(self.datas)
        self.db.memory_databases[self.name] = datas
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("memory::" + self.name)

    def verbose(self):
        return "datas(%d): \n%s" % (len(self.datas), human_repr_object(self.datas))


class MemoryUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryUpdateBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query.append((key, '>', value, Cmper.cmp_gt))

    def filter_gte(self, key, value):
        self.query.append((key, ">=", value, Cmper.cmp_gte))

    def filter_lt(self, key, value):
        self.query.append((key, "<", value, Cmper.cmp_lt))

    def filter_lte(self, key, value):
        self.query.append((key, "<=", value, Cmper.cmp_lte))

    def filter_eq(self, key, value):
        self.query.append((key, "==", value, Cmper.cmp_eq))

    def filter_ne(self, key, value):
        self.query.append((key, "!=", value, Cmper.cmp_ne))

    def filter_in(self, key, value):
        try:
            self.query.append((key, "in", set(value) if isinstance(value, list) else value, Cmper.cmp_in))
        except:
            self.query.append((key, "in", value, Cmper.cmp_in))

    def commit(self):
        if ".--" in self.name:
            cache_keys = self.name.split(".")
            if len(cache_keys) == 2 or cache_keys[1][:2] == "--":
                return

        datas = []
        for data in self.db.memory_databases.get(self.name, []):
            succed = True
            for key, exp, value, cmp in self.query:
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
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("memory::" + self.name)
        return datas

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (
            human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query]),
            human_repr_object(self.diff_data))


class MemoryDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(MemoryDeleteBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query.append((key, '>', value, Cmper.cmp_gt))

    def filter_gte(self, key, value):
        self.query.append((key, ">=", value, Cmper.cmp_gte))

    def filter_lt(self, key, value):
        self.query.append((key, "<", value, Cmper.cmp_lt))

    def filter_lte(self, key, value):
        self.query.append((key, "<=", value, Cmper.cmp_lte))

    def filter_eq(self, key, value):
        self.query.append((key, "==", value, Cmper.cmp_eq))

    def filter_ne(self, key, value):
        self.query.append((key, "!=", value, Cmper.cmp_ne))

    def filter_in(self, key, value):
        try:
            self.query.append((key, "in", set(value) if isinstance(value, list) else value, Cmper.cmp_in))
        except:
            self.query.append((key, "in", value, Cmper.cmp_in))

    def commit(self):
        if ".--" in self.name:
            cache_keys = self.name.split(".")
            if len(cache_keys) == 2 or cache_keys[1][:2] == "--":
                return

        if not self.query:
            self.db.memory_databases.pop(self.name, None)
            return []

        datas = []
        for data in self.db.memory_databases.get(self.name, []):
            succed = True
            for key, exp, value, cmp in self.query:
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if not succed:
                datas.append(data)

        self.db.memory_databases[self.name] = datas
        tasker_context = TaskerContext.current()
        if tasker_context:
            tasker_context.remove_iterator("memory::" + self.name)
        return datas

    def verbose(self):
        return "filters: %s" % human_repr_object([(key, exp, value) for key, exp, value, cmp in self.query])


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


class MemoryDBCollection(dict):
    def remove(self, name):
        self.pop(name, None)


class MemoryDBFactory(DatabaseFactory):
    def __init__(self, *args, **kwargs):
        super(MemoryDBFactory, self).__init__(*args, **kwargs)

        self.memory_collection = None

    def create(self):
        if not self.memory_collection:
            self.memory_collection = MemoryDBCollection()
        return self.memory_collection

    def ping(self, driver):
        return True

    def close(self, driver):
        pass


class MemoryDB(DataBase):
    def __init__(self, manager, config):
        super(MemoryDB, self).__init__(manager, dict(**config))

        self.memory_databases = None

    def build_factory(self):
        return MemoryDBFactory(self.get_config_key(), self.config)

    def ensure_memory_databases(self):
        self.memory_databases = self.acquire_driver().raw()
        self.release_driver()

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

    def is_dynamic_schema(self, name):
        return True