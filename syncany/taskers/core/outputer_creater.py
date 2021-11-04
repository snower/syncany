# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from ...errors import OutputerUnknownException

class OutputerCreater(object):
    def __init__(self, tasker):
        self.tasker = tasker

    @property
    def databases(self):
        return self.tasker.databases

    def can_uses(self):
        return [
            "db_update_delete_insert_outputer",
            "db_update_insert_outputer",
            "db_delete_insert_outputer",
            "db_insert_outputer"
        ]

    def find_outputer_driver(self, *args, **kwargs):
        return self.tasker.find_outputer_driver(*args, **kwargs)

    def create_db_update_delete_insert_outputer(self, config, primary_keys):
        outputer_cls = self.find_outputer_driver(config["name"])
        if not outputer_cls:
            raise OutputerUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys,
                            insert_batch=self.tasker.arguments.get("@insert_batch", 0))

    def create_db_update_insert_outputer(self, config, primary_keys):
        outputer_cls = self.find_outputer_driver(config["name"])
        if not outputer_cls:
            raise OutputerUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys,
                            insert_batch=self.tasker.arguments.get("@insert_batch", 0),
                            join_batch=self.tasker.arguments.get("@join_batch", 1000))

    def create_db_delete_insert_outputer(self, config, primary_keys):
        outputer_cls = self.find_outputer_driver(config["name"])
        if not outputer_cls:
            raise OutputerUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys,
                            insert_batch=self.tasker.arguments.get("@insert_batch", 0))

    def create_db_insert_outputer(self, config, primary_keys):
        outputer_cls = self.find_outputer_driver(config["name"])
        if not outputer_cls:
            raise OutputerUnknownException(config["name"] + " is unknown")
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys,
                            insert_batch=self.tasker.arguments.get("@insert_batch", 0))