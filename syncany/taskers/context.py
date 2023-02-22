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

    def __init__(self):
        self.caches = defaultdict(TaskerContextCache)

    def flush(self):
        self.caches.clear()

    def close(self):
        self.caches.clear()

    def cache(self, key):
        return self.caches[key]
