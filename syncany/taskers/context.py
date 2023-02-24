# -*- coding: utf-8 -*-
# 2023/2/22
# create by: snower

from collections import defaultdict


class TaskerContextCache(dict):
    pass
TaskerContextCache.set = TaskerContextCache.__setitem__


class TaskerContext(object):
    _thread_local = None

    @classmethod
    def current(cls):
        try:
            return cls._thread_local.current_tasker.context
        except AttributeError:
            return None

    @property
    def tasker(self):
        return self._thread_local.current_tasker

    def __init__(self):
        self.caches = defaultdict(TaskerContextCache)
        self.iterators = {}

    def flush(self):
        self.caches.clear()

    def reset(self):
        for name, iterator in self.iterators.items():
            iterator.close()
        self.iterators.clear()

    def close(self):
        self.flush()
        self.reset()

    def cache(self, key):
        return self.caches[key]

    def add_iterator(self, name, iterator):
        if name in self.iterators:
            self.iterators[name].close()
        self.iterators[name] = iterator

    def remove_iterator(self, name):
        if name not in self.iterators:
            return
        iterator = self.iterators.pop(name)
        iterator.close()

    def get_iterator(self, name):
        return self.iterators.get(name)
