# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from ...outputers import find_outputer
from ...errors import OutputerUnknownException

class OutputerCreater(object):
    def create_db_update_delete_insert_outputer(self, config, primary_keys):
        outputer_cls = find_outputer(config["name"])
        if not outputer_cls:
            raise OutputerUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys)

    def create_db_update_insert_outputer(self, config, primary_keys):
        outputer_cls = find_outputer(config["name"])
        if not outputer_cls:
            raise OutputerUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys)

    def create_db_delete_insert_outputer(self, config, primary_keys):
        outputer_cls = find_outputer(config["name"])
        if not outputer_cls:
            raise OutputerUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys)

    def create_db_insert_outputer(self, config, primary_keys):
        outputer_cls = find_outputer(config["name"])
        if not outputer_cls:
            raise OutputerUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys)