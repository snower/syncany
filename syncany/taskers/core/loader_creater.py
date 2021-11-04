# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from ...errors import LoaderUnknownException

class LoaderCreater(object):
    def __init__(self, tasker):
        self.tasker = tasker

    @property
    def databases(self):
        return self.tasker.databases

    def can_uses(self):
        return [
            "db_loader",
            "db_pull_loader",
        ]

    def find_loader_driver(self, *args, **kwargs):
        return self.tasker.find_loader_driver(*args, **kwargs)

    def create_const_loader(self, config, primary_keys):
        loader_cls = self.find_loader_driver(config["name"])
        if not loader_cls:
            raise LoaderUnknownException(config["name"] + " is unknown")
        return loader_cls(config["datas"], primary_keys, is_yield=config.get("is_yield", False))

    def create_db_loader(self, config, primary_keys):
        loader_cls = self.find_loader_driver(config["name"])
        if not loader_cls:
            raise LoaderUnknownException(config["name"] + " is unknown")

        db_name = config["database"].split(".")[0]
        return loader_cls(self.databases[db_name], config["database"], primary_keys,
                          is_yield=config.get("is_yield", False))

    def create_db_join_loader(self, config, primary_keys):
        loader_cls = self.find_loader_driver(config["name"])
        if not loader_cls:
            raise LoaderUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return loader_cls(self.databases[db_name], config["database"], primary_keys,
                          is_yield=config.get("is_yield", False),
                          join_batch=self.tasker.arguments.get("@join_batch", 1000))

    def create_db_pull_loader(self, config, primary_keys):
        loader_cls = self.find_loader_driver(config["name"])
        if not loader_cls:
            raise LoaderUnknownException(config["name"] + " is unknown")

        db_name = config["database"].split(".")[0]
        return loader_cls(self.databases[db_name], config["database"], primary_keys,
                          is_yield=config.get("is_yield", False))