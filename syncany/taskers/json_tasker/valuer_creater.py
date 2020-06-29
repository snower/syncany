# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from ...valuers import find_valuer
from ...valuers.valuer import LoadAllFieldsException
from ...filters import find_filter
from ...calculaters import find_calculater

class LoaderJoinWarp(object):
    __loader = None

    def __init__(self, loader):
        self.__origin_loader = loader
        self.__loader = loader

    def __getattr__(self, item):
        if self.__loader is None or item in ("_LoaderJoinWarp__origin_loader", "_LoaderJoinWarp__loader"):
            return super(LoaderJoinWarp, self).__getattr__(item)

        return getattr(self.__loader, item)

    def __setattr__(self, key, value):
        if self.__loader is None or key in ("_LoaderJoinWarp__origin_loader", "_LoaderJoinWarp__loader"):
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
    def create_const_valuer(self, config, **kwargs):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        filter_cls = find_filter(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None
        return valuer_cls(config["value"], "", filter)

    def create_db_valuer(self, config, **kwargs):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        filter_cls = find_filter(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None
        return valuer_cls(config["key"], filter)

    def create_inherit_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        filter_cls = find_filter(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None
        value_valuer = self.create_valuer(config["valuer"], inherit_valuers=inherit_valuers, **kwargs)
        inherit_valuer = valuer_cls(value_valuer, config["key"], filter)
        if inherit_valuers is not None:
            inherit_valuers.append({
                "reflen": config["reflen"],
                "valuer": inherit_valuer,
            })
        return inherit_valuer.get_inherit_child_valuer()

    def create_const_join_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        loader = self.create_loader(config["loader"], [config["foreign_key"]])

        child_inherit_valuers = []
        child_valuer = self.create_valuer(config["valuer"], inherit_valuers=child_inherit_valuers, **kwargs)

        if config["foreign_key"] not in loader.schema:
            loader.add_valuer(config["foreign_key"], self.create_valuer(self.compile_db_valuer(config["foreign_key"], None)))
        for key in child_valuer.get_fields():
            if key not in loader.schema:
                loader.add_valuer(key, self.create_valuer(self.compile_db_valuer(key, None)))

        current_inherit_valuers = []
        for inherit_valuer in child_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)
        return valuer_cls(loader, config["foreign_key"], child_valuer, current_inherit_valuers,
                          config["value"], config["key"], None)

    def create_db_join_valuer(self, config, inherit_valuers=None, join_loaders=None, **kwargs):
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

        args_valuer = self.create_valuer(config["args_valuer"], inherit_valuers=inherit_valuers,
                                         join_loaders=join_loaders, **kwargs) if config["args_valuer"] else None
        child_inherit_valuers = []
        child_valuer = self.create_valuer(config["valuer"], inherit_valuers=child_inherit_valuers, join_loaders=join_loaders, **kwargs)
        filter_cls = find_filter(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        if config["foreign_key"] not in loader.schema:
            loader.add_valuer(config["foreign_key"], self.create_valuer(self.compile_db_valuer(config["foreign_key"], None)))

        current_inherit_valuers = []
        try:
            for key in child_valuer.get_fields():
                if key not in loader.schema:
                    loader.add_valuer(key, self.create_valuer(self.compile_db_valuer(key, None)))

            for inherit_valuer in child_inherit_valuers:
                inherit_valuer["reflen"] -= 1
                if inherit_valuer["reflen"] == 0:
                    current_inherit_valuers.append(inherit_valuer["valuer"])
                elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                    inherit_valuers.append(inherit_valuer)
        except LoadAllFieldsException:
            loader.schema.clear()
            loader.add_key_matcher(".*", self.create_valuer(self.compile_db_valuer("", None)))
        return valuer_cls(loader, config["foreign_key"], config["foreign_filters"], args_valuer, child_valuer,
                          current_inherit_valuers, config["key"], filter)

    def create_case_valuer(self, config, **kwargs):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return

        if "value" in config and config["value"]:
            value_valuer = self.create_valuer(config["value"], **kwargs)
        else:
            value_valuer = None

        case_valuers = {}
        for key, valuer_config in config["case"].items():
            case_valuers[key] = self.create_valuer(valuer_config, **kwargs)
        default_case_valuer = self.create_valuer(config["default_case"], **kwargs) \
            if "default_case" in config and config["default_case"] else None
        return valuer_cls(case_valuers, default_case_valuer, value_valuer, config["key"], None)

    def create_calculate_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return

        args_valuers = []
        for valuer_config in config["args"]:
            args_valuers.append(self.create_valuer(valuer_config, inherit_valuers=inherit_valuers, **kwargs))
        calculater = find_calculater(config["key"])

        filter_cls = find_filter(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return" in config and config["return"] else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(calculater, args_valuers, return_valuer, current_inherit_valuers, "", filter)

    def create_schema_valuer(self, config, **kwargs):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        schema_valuers = {}
        for key, valuer_config in config["schema"].items():
            schema_valuers[key] = self.create_valuer(valuer_config, **kwargs)
        return valuer_cls(schema_valuers, config["key"], None)