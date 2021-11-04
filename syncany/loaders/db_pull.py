# -*- coding: utf-8 -*-
# 2021/3/3
# create by: snower

import time
import threading
from .db import DBLoader

class DBPullLoader(DBLoader):
    def __init__(self, *args, **kwargs):
        super(DBPullLoader, self).__init__(*args, **kwargs)

        self.last_load_time = 0
        self.start_loaded = False
        self.wait_event = None

    def clone(self):
        loader = super(DBPullLoader, self).clone()
        loader.last_load_time = self.last_load_time
        loader.start_loaded = self.start_loaded
        loader.wait_event = self.wait_event
        return loader

    def next(self):
        self.start_loaded = False
        return True

    def load(self, timeout=None):
        if self.loaded:
            return
        if self.start_loaded:
            return super(DBPullLoader, self).load(timeout)

        timeout = timeout or 60
        if self.last_load_time:
            now = time.time()
            if now - self.last_load_time < timeout:
                if self.wait_event is None:
                    self.wait_event = threading.Event()
                self.wait_event.clear()
                timeout = timeout - (now - self.last_load_time)
                if self.wait_event.wait(timeout):
                    raise SystemError("db pull loader terminated")

        self.last_load_time = time.time()
        self.start_loaded = True
        return super(DBPullLoader, self).load(timeout)

    def terminate(self):
        if not self.wait_event:
            return
        self.wait_event.set()