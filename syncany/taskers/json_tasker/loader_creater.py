# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from ...loaders import find_loader
from ...errors import LoaderUnknownException

class LoaderCreater(object):
    def create_const_loader(self, config, primary_keys):
        loader_cls = find_loader(config["name"])
        if not loader_cls:
            raise LoaderUnknownException(config["name"] + " is unknown")
        return loader_cls(config["datas"], primary_keys, is_yield=config.get("is_yield", False))

    def create_db_loader(self, config, primary_keys):
        loader_cls = find_loader(config["name"])
        if not loader_cls:
            raise LoaderUnknownException(config["name"] + " is unknown")

        db_name = config["database"].split(".")[0]
        return loader_cls(self.databases[db_name], config["database"], primary_keys,
                          is_yield=config.get("is_yield", False))

    def create_db_join_loader(self, config, primary_keys):
        loader_cls = find_loader(config["name"])
        if not loader_cls:
            raise LoaderUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return loader_cls(self.databases[db_name], config["database"], primary_keys,
                          is_yield=config.get("is_yield", False))