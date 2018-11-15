# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from ...valuers import find_valuer
from ...filters import find_filter
from ...calculaters import find_calculater

class LoaderJoinWarp(object):
    __loader = None

    def __init__(self, loader):
        self.__origin_loader = loader
        self.__loader = loader

    def __getattr__(self, item):
        if self.__loader is None:
            return super(LoaderJoinWarp, self).__getattr__(item)

        return getattr(self.__loader, item)

    def __setattr__(self, key, value):
        if self.__loader is None:
            return super(LoaderJoinWarp, self).__setattr__(key, value)

        return setattr(self.__loader, key, value)

    def __str__(self):
        if self.__loader is None:
            return super(LoaderJoinWarp, self).__str__()

        return str(self.__loader)

    def __repr__(self):
        if self.__loader is None:
            return super(LoaderJoinWarp, self).__repr__()

        return repr(self.__loader)

    def clone(self):
        self.__loader = self.__origin_loader.clone()
        return self

class ValuerCreater(object):
    def create_const_valuer(self, config, join_loaders = None):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        return valuer_cls(config["value"], "")

    def create_db_valuer(self, config, join_loaders = None):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        filter_cls = find_filter(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None
        return valuer_cls(config["key"], filter)

    def create_const_join_valuer(self, config, join_loaders = None):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        loader = self.create_loader(config["loader"], [config["foreign_key"]])
        child_valuer = self.create_valuer(config["valuer"], join_loaders)

        if config["foreign_key"] not in loader.schema:
            loader.add_valuer(config["foreign_key"],
                              self.create_valuer(self.compile_db_valuer(config["foreign_key"], None), join_loaders))
        for key in child_valuer.get_fields():
            if key not in loader.schema:
                loader.add_valuer(key, self.create_valuer(self.compile_db_valuer(key, None), join_loaders))
        return valuer_cls(loader, config["foreign_key"], child_valuer, config["value"], config["key"], None)

    def create_db_join_valuer(self, config, join_loaders = None):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        if join_loaders is not None:
            if config["foreign_filters"]:
                loader_cache_foreign_filters = "&".join(sorted(["%s %s %s" % (name, exp, str(value)) for name, exp, value in config["foreign_filters"]]))
            else:
                loader_cache_foreign_filters = ""
            loader_cache_key = config["loader"]["database"] + "::" + config["foreign_key"] + "::" + loader_cache_foreign_filters
            if loader_cache_key in join_loaders:
                loader = join_loaders[loader_cache_key]
            else:
                loader = self.create_loader(config["loader"], [config["foreign_key"]])
                if config["foreign_filters"]:
                    for name, exp, value in config["foreign_filters"]:
                        if exp == "eq":
                            loader.add_filter(name, exp, value)
                        else:
                            getattr(loader, "filter_" + exp)(name, value)
                loader = LoaderJoinWarp(loader)
                join_loaders[loader_cache_key] = loader
        else:
            loader = self.create_loader(config["loader"], [config["foreign_key"]])

        child_valuer = self.create_valuer(config["valuer"], join_loaders)
        filter_cls = find_filter(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        if config["foreign_key"] not in loader.schema:
            loader.add_valuer(config["foreign_key"],
                              self.create_valuer(self.compile_db_valuer(config["foreign_key"], None), join_loaders))
        for key in child_valuer.get_fields():
            if key not in loader.schema:
                loader.add_valuer(key, self.create_valuer(self.compile_db_valuer(key, None), join_loaders))
        return valuer_cls(loader, config["foreign_key"], config["foreign_filters"], child_valuer, config["key"], filter)

    def create_case_valuer(self, config, join_loaders = None):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return

        if "value" in config and config["value"]:
            value_valuer = self.create_valuer(config["value"], join_loaders)
        else:
            value_valuer = None

        case_valuers = {}
        for key, valuer_config in config["case"].items():
            case_valuers[key] = self.create_valuer(valuer_config, join_loaders)
        default_case_valuer = self.create_valuer(config["default_case"], join_loaders) \
            if "default_case" in config and config["default_case"] else None
        return valuer_cls(case_valuers, default_case_valuer, value_valuer, config["key"], None)

    def create_calculate_valuer(self, config, join_loaders=None):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return

        args_valuers = []
        for valuer_config in config["args"]:
            args_valuers.append(self.create_valuer(valuer_config, join_loaders))
        calculater = find_calculater(config["key"])

        filter_cls = find_filter(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        return_valuer = self.create_valuer(config["return"], join_loaders) if "return" in config and config["return"] else None

        return valuer_cls(calculater, args_valuers, return_valuer, "", filter)

    def create_schema_valuer(self, config, join_loaders=None):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        schema_valuers = {}
        for key, valuer_config in config["schema"].items():
            schema_valuers[key] = self.create_valuer(valuer_config, join_loaders)
        return valuer_cls(schema_valuers, config["key"], None)