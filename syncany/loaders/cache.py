# -*- coding: utf-8 -*-
# 2021/11/8
# create by: snower


class CacheLoader(object):
    def __init__(self, name, db, config):
        self.name = name
        self.db = db
        self.cache_builder = db.cache(name, config.get("prefix_key", "syncany"), config)
        self.exprie_seconds = config.get("exprie_seconds", 86400)
        self.loaded_caches = {}

    def get(self, key):
        if key in self.loaded_caches:
            return self.loaded_caches[key]
        return self.cache_builder.get(key)

    def set(self, key, value):
        self.cache_builder.set(key, value, self.exprie_seconds)
        self.loaded_caches[key] = value

    def delete(self, key):
        if key in self.loaded_caches:
            self.loaded_caches.pop(key)
        self.cache_builder.delete(key)