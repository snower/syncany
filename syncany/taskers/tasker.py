# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import time
import threading
from ..utils import gen_runner_id
from ..loaders import find_loader, Loader
from ..outputers import find_outputer, Outputer
from ..valuers import find_valuer, Valuer
from ..filters import find_filter, Filter
from ..database import find_database, DataBase
from ..calculaters import find_calculater, Calculater
from ..hook import Hooker

_thread_local = threading.local()


class TaskerStatus(dict):
    def __init__(self):
        super(TaskerStatus, self).__init__(
            runner_id=None,
            start_time=time.time(),
            status="running",
            message="",
            trackback=None,
            data={
                "first": None,
                "last": None
            },
            statistics={
                "loader": {},
                "join_loaders": {},
                "outputer": {}
            }
        )

    @property
    def runner_id(self):
        if "runner_id" in self and self["runner_id"]:
            return self["runner_id"]
        self["runner_id"] = gen_runner_id()
        return self["runner_id"]

    @property
    def start_time(self):
        return self["start_time"]


class Tasker(object):
    name = ""

    def __init__(self, manager, parent=None):
        self.manager = manager
        self.parent = parent
        self.status = TaskerStatus()
        self.extensions = {
            "loaders": {},
            "outputers": {},
            "valuers": {},
            "filters": {},
            "databases": [],
            "caches": [],
            "calculaters": {},
        }
        self.arguments = {}
        self.states = {}
        self.input = ""
        self.output = ""
        self.databases = {}
        self.caches = {}
        self.schema = {}
        self.loader = None
        self.outputer = None
        self.hookers = set([])

    def find_loader_driver(self, name):
        if name in self.extensions["loaders"]:
            return self.extensions["loaders"][name]
        return find_loader(name)

    def find_outputer_driver(self, name):
        if name in self.extensions["outputers"]:
            return self.extensions["outputers"][name]
        return find_outputer(name)

    def find_valuer_driver(self, name):
        if name in self.extensions["valuers"]:
            return self.extensions["valuers"][name]
        return find_valuer(name)

    def find_filter_driver(self, name):
        if name in self.extensions["filters"]:
            return self.extensions["filters"][name]
        return find_filter(name)

    def find_database_driver(self, name):
        if name in self.extensions["databases"]:
            return self.extensions["databases"][name]
        return find_database(name)

    def find_calculater_driver(self, name):
        name = name.split("::")[0]
        if name in self.extensions["calculaters"]:
            return self.extensions["calculaters"][name]
        return find_calculater(name)

    def register_loader_driver(self, name, loader):
        if not issubclass(loader, Loader):
            raise TypeError("is not Loader")
        self.extensions["loaders"][name] = loader
        return loader

    def register_outputer_driver(self, name, outputer):
        if not issubclass(outputer, Outputer):
            raise TypeError("is not Outputer")
        self.extensions["outputers"][name] = outputer
        return outputer

    def register_valuer_driver(self, name, valuer):
        if not issubclass(valuer, Valuer):
            raise TypeError("is not Valuer")
        self.extensions["valuers"][name] = valuer
        return valuer

    def register_filter_driver(self, name, filter):
        if not issubclass(filter, Filter):
            raise TypeError("is not Filter")
        self.extensions["filters"][name] = filter
        return filter

    def register_database_driver(self, name, database):
        if not issubclass(database, DataBase):
            raise TypeError("is not DataBase")
        self.extensions["databases"][name] = database
        return database

    def register_calculater_driver(self, name, calculater):
        if not issubclass(calculater, Calculater):
            raise TypeError("is not Calculater")
        self.extensions["calculaters"][name] = calculater
        return calculater

    def add_hooker(self, hooker):
        if not isinstance(hooker, Hooker):
            raise TypeError("is not Hooker instance")
        self.hookers.add(hooker)

    def get_loader(self):
        pass

    def load(self):
        _thread_local.current_tasker = self

    def compile(self, arguments):
        _thread_local.current_tasker = self
        self.arguments = arguments

    def run(self):
        _thread_local.current_tasker = self

    def decorator_compiled(self, func):
        hooker = Hooker()
        hooker.compiled = func
        self.add_hooker(hooker)
        return func

    def decorator_queried(self, func):
        hooker = Hooker()
        hooker.queried = func
        self.add_hooker(hooker)
        return func

    def decorator_loaded(self, func):
        hooker = Hooker()
        hooker.loaded = func
        self.add_hooker(hooker)
        return func

    def decorator_outputed(self, func):
        hooker = Hooker()
        hooker.outputed = func
        self.add_hooker(hooker)
        return func

def current_tasker():
    try:
        return _thread_local.current_tasker
    except AttributeError:
        return None