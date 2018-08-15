# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from ...loaders import find_loader

class LoaderCreater(object):
    def create_const_loader(self, config, primary_keys):
        loader_cls = find_loader(config["name"])
        if not loader_cls:
            return None
        return loader_cls(config["datas"], config["database"], primary_keys)

    def create_db_loader(self, config, primary_keys):
        loader_cls = find_loader(config["name"])
        if not loader_cls:
            return None

        db_name = config["database"].split(".")[0]
        return loader_cls(self.databases[db_name], config["database"], primary_keys)

    def create_db_join_loader(self, config, primary_keys):
        loader_cls = find_loader(config["name"])
        if not loader_cls:
            return None
        db_name = config["database"].split(".")[0]
        return loader_cls(self.databases[db_name], config["database"], primary_keys)