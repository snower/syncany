# -*- coding: utf-8 -*-
# 2021/11/8
# create by: snower

from ..database import DatabaseInstanceBuilder


class CacheLoader(object):
    def __init__(self, name, db, config):
        self.name = name
        self.db = db
        self.prefix_key = "syncany"
        self.exprie_seconds = 86400
        self.config = config
        self.cache_builder = None
        self.loaded_caches = {}

    def init_build(self):
        self.prefix_key = self.config.pop("prefix_key", "syncany")
        self.exprie_seconds = int(self.config.pop("exprie_seconds", 86400))
        if isinstance(self.db, DatabaseInstanceBuilder):
            self.db = self.db.build()
        self.cache_builder = self.db.cache(self.name, self.prefix_key, self.config)

    def get(self, key):
        if key in self.loaded_caches:
            return self.loaded_caches[key]
        if not self.cache_builder:
            self.init_build()
        return self.cache_builder.get(key)

    def set(self, key, value):
        if not self.cache_builder:
            self.init_build()
        self.cache_builder.set(key, value, self.exprie_seconds)
        self.loaded_caches[key] = value

    def delete(self, key):
        if key in self.loaded_caches:
            self.loaded_caches.pop(key)
        if not self.cache_builder:
            self.init_build()
        self.cache_builder.delete(key)