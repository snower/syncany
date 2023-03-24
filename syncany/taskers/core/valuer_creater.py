# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from ...valuers.valuer import LoadAllFieldsException
from ...errors import CacheUnknownException, ValuerUnknownException


class LoaderJoinWarp(object):
    __loader = None

    def __init__(self, loader):
        self.__loader = loader

    def __getattr__(self, item):
        if self.__loader is None or item == "_LoaderJoinWarp__loader":
            return super(LoaderJoinWarp, self).__getattr__(item)
        return getattr(self.__loader, item)

    def __setattr__(self, key, value):
        if self.__loader is None or key == "_LoaderJoinWarp__loader":
            return super(LoaderJoinWarp, self).__setattr__(key, value)
        return setattr(self.__loader, key, value)

    def __str__(self):
        return str(self.__loader)

    def __repr__(self):
        return repr(self.__loader)

    def clone(self):
        self.__loader = self.__loader.clone()
        return self

    def add_valuer(self, name, valuer):
        return self.__loader.add_valuer(name, valuer)

    def add_intercept(self, intercept):
        return self.__loader.add_intercept(intercept)

    def add_key_matcher(self, matcher, valuer):
        return self.__loader.add_key_matcher(matcher, valuer)

    def get_data_primary_key(self, data):
        return self.__loader.get_data_primary_key(data)

    def next(self):
        return self.__loader.next()

    def is_dynamic_schema(self):
        return self.__loader.is_dynamic_schema()

    def is_streaming(self):
        return self.__loader.is_streaming()

    def set_streaming(self, is_streaming=None):
        return self.__loader.set_streaming(is_streaming)

    def create_group_macther(self, return_valuer):
        return self.__loader.create_group_macther(return_valuer)

    def create_macther(self, keys, values):
        return self.__loader.create_macther(keys, values)

    def try_load(self):
        return self.__loader.try_load()

    def load(self, timeout=None):
        return self.__loader.load(timeout)

    def statistics(self):
        return self.__loader.statistics()

class ValuerCreater(object):
    def __init__(self, tasker):
        self.tasker = tasker

    def find_valuer_driver(self, *args, **kwargs):
        return self.tasker.find_valuer_driver(*args, **kwargs)

    def find_filter_driver(self, *args, **kwargs):
        return self.tasker.find_filter_driver(*args, **kwargs)

    def find_calculater_driver(self, *args, **kwargs):
        return self.tasker.find_calculater_driver(*args, **kwargs)

    def create_valuer(self, *args, **kwargs):
        return self.tasker.create_valuer(*args, **kwargs)

    def create_loader(self, *args, **kwargs):
        return self.tasker.create_loader(*args, **kwargs)

    def create_join_loader(self, config, join_loaders, renew=False):
        def format_value_cache_key(value):
            if isinstance(value, dict):
                return "{" + ", ".join(["%s: %s" % (format_value_cache_key(key), format_value_cache_key(value[key]))
                                        for key in sorted(value.keys())]) + "}"
            if isinstance(value, (tuple, list, set)):
                try:
                    return "[" + ", ".join([format_value_cache_key(v) for v in sorted(list(value))]) + "]"
                except:
                    return "[" + ", ".join([format_value_cache_key(v) for v in value]) + "]"
            return str(value)

        loader_cache_key, loader_cache_foreign_filters = None, ""
        if join_loaders is not None:
            if config["foreign_filters"]:
                loader_cache_foreign_filters = "&".join(sorted(["%s__%s=%s" % (name, exp, format_value_cache_key(valuer))
                                                                for name, exp, valuer, _, _ in config["foreign_filters"]]))
            loader_cache_key = "::".join([config["loader"]["name"], config["loader"]["database"],
                                          "+".join(sorted(config["foreign_keys"])),
                                          loader_cache_foreign_filters])
            if not renew and loader_cache_key in join_loaders:
                return join_loaders[loader_cache_key]

        loader = self.create_loader(config["loader"], config["foreign_keys"])
        if config["foreign_filters"]:
            for name, exp, valuer, filter_cls, filter_args in config["foreign_filters"]:
                inherit_valuers, yield_valuers, aggregate_valuers = [], [], []
                valuer = self.create_valuer(valuer, schema_field_name="",
                                            inherit_valuers=inherit_valuers, join_loaders=self.tasker.join_loaders,
                                            yield_valuers=yield_valuers, aggregate_valuers=aggregate_valuers,
                                            define_valuers={},
                                            global_variables=dict(**self.tasker.config["variables"]),
                                            global_states=self.tasker.states)

                def add_foreign_filter(name, exp, valuer):
                    def _():
                        value = self.tasker.execute_valuer(valuer, self.tasker.arguments)
                        if filter_cls:
                            if exp == "in" and isinstance(value, list):
                                value = [filter_cls(filter_args).filter(v) for v in value]
                            else:
                                value = filter_cls(filter_args).filter(value)
                        if exp == "eq":
                            loader.add_filter(name, exp, value)
                        else:
                            getattr(loader, "filter_" + exp)(name, value)
                    return _
                self.tasker.add_init_executer(add_foreign_filter(name, exp, valuer))
        loader = LoaderJoinWarp(loader)
        if loader_cache_key is not None:
            if loader_cache_key in join_loaders:
                join_loaders[loader_cache_key + "#" + str(id(join_loaders[loader_cache_key]))] = join_loaders[loader_cache_key]
            join_loaders[loader_cache_key] = loader
        return loader

    def compile_data_valuer(self, *args, **kwargs):
        return self.tasker.valuer_compiler.compile_data_valuer(*args, **kwargs)

    def create_const_valuer(self, config, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None
        return valuer_cls(config["value"], "", filter)

    def create_data_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(return_valuer, current_inherit_valuers, config["key"], filter)

    def create_inherit_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None
        value_valuer = self.create_valuer(config["value_valuer"], inherit_valuers=inherit_valuers, **kwargs)
        inherit_valuer = valuer_cls(value_valuer, config["key"], filter)
        if inherit_valuers is not None:
            inherit_valuers.append({
                "reflen": config["reflen"],
                "valuer": inherit_valuer,
            })
        return inherit_valuer.get_inherit_child_valuer()

    def create_db_load_valuer(self, config, inherit_valuers=None, join_loaders=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        loader = self.create_join_loader(config, join_loaders)
        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, join_loaders=join_loaders, **kwargs)
        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        for foreign_key in config["foreign_keys"]:
            if foreign_key not in loader.schema:
                if loader.loaded:
                    loader = self.create_join_loader(config, join_loaders, True)
                loader.add_valuer(foreign_key, self.create_valuer(self.compile_data_valuer(foreign_key, None)))

        try:
            for key in return_valuer.get_fields():
                if key not in loader.schema:
                    if loader.loaded:
                        loader = self.create_join_loader(config, join_loaders, True)
                    loader.add_valuer(key, self.create_valuer(self.compile_data_valuer(key, None)))
        except LoadAllFieldsException:
            if loader.loaded:
                loader = self.create_join_loader(config, join_loaders, True)
            loader.schema.clear()
            loader.add_key_matcher(".*", self.create_valuer(self.compile_data_valuer("", None)))

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(loader, config["foreign_keys"], config["foreign_filters"], return_valuer,
                          current_inherit_valuers, config["key"], filter)

    def create_db_join_valuer(self, config, inherit_valuers=None, join_loaders=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        loader = self.create_join_loader(config, join_loaders)
        args_valuers = [self.create_valuer(args_valuer, inherit_valuers=inherit_valuers,
                                           join_loaders=join_loaders, **kwargs) for args_valuer in config["args_valuers"]] \
            if config["args_valuers"] else None
        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, join_loaders=join_loaders, **kwargs)
        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        for foreign_key in config["foreign_keys"]:
            if foreign_key not in loader.schema:
                if loader.loaded:
                    loader = self.create_join_loader(config, join_loaders, True)
                loader.add_valuer(foreign_key, self.create_valuer(self.compile_data_valuer(foreign_key, None)))

        try:
            for key in return_valuer.get_fields():
                if key not in loader.schema:
                    if loader.loaded:
                        loader = self.create_join_loader(config, join_loaders, True)
                    loader.add_valuer(key, self.create_valuer(self.compile_data_valuer(key, None)))
        except LoadAllFieldsException:
            if loader.loaded:
                loader = self.create_join_loader(config, join_loaders, True)
            loader.schema.clear()
            loader.add_key_matcher(".*", self.create_valuer(self.compile_data_valuer("", None)))

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(loader, config["foreign_keys"], config["foreign_filters"], args_valuers, return_valuer,
                          current_inherit_valuers, config["key"], filter)

    def create_case_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        if "value_valuer" in config and config["value_valuer"]:
            value_valuer = self.create_valuer(config["value_valuer"], inherit_valuers=inherit_valuers, **kwargs)
        else:
            value_valuer = None

        case_valuers = {}
        for key, valuer_config in config["case_valuers"].items():
            case_valuers[key] = self.create_valuer(valuer_config, inherit_valuers=inherit_valuers, **kwargs)

        default_case_valuer = self.create_valuer(config["default_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "default_valuer" in config and config["default_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(case_valuers, default_case_valuer, value_valuer, return_valuer, current_inherit_valuers, config["key"], None)

    def create_calculate_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        args_valuers = []
        for valuer_config in config["args_valuers"]:
            args_valuers.append(self.create_valuer(valuer_config, inherit_valuers=inherit_valuers, **kwargs))
        calculater = self.find_calculater_driver(config["key"])

        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(calculater.instance(config['key']), args_valuers, return_valuer, current_inherit_valuers, "", filter)

    def create_schema_valuer(self, config, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        schema_valuers = {}
        for key, valuer_config in config["schema_valuers"].items():
            schema_valuers[key] = self.create_valuer(valuer_config, **kwargs)
        return valuer_cls(schema_valuers, config["key"], None)

    def create_make_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        if isinstance(config["value_valuer"], dict):
            if "name" in config["value_valuer"] and isinstance(config["value_valuer"]["name"], str):
                value_valuer = self.create_valuer(config["value_valuer"], inherit_valuers=inherit_valuers, **kwargs)
            else:
                value_valuer = {key: (self.create_valuer(key_config, inherit_valuers=inherit_valuers, **kwargs),
                                      self.create_valuer(value_config, inherit_valuers=inherit_valuers, **kwargs))
                                for key, (key_config, value_config) in config["value_valuer"].items()}
        elif isinstance(config["value_valuer"], list):
            value_valuer = [self.create_valuer(value_config, inherit_valuers=inherit_valuers, **kwargs)
                            for value_config in config["value_valuer"]]
        else:
            value_valuer = None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)
        return valuer_cls(value_valuer, return_valuer, current_inherit_valuers, config["key"], None)

    def create_let_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        key_valuer = self.create_valuer(config["key_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "key_valuer" in config and config["key_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)
        return valuer_cls(key_valuer, return_valuer, current_inherit_valuers, config["key"], filter)

    def create_yield_valuer(self, config, inherit_valuers=None, yield_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        value_valuer = self.create_valuer(config["value_valuer"], inherit_valuers=inherit_valuers,
                                          yield_valuers=yield_valuers, **kwargs) \
            if "value_valuer" in config and config["value_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers,
                                           yield_valuers=yield_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)
        yield_valuer = valuer_cls(value_valuer, return_valuer, current_inherit_valuers, config["key"], filter)
        if yield_valuers is not None:
            yield_valuers.append(yield_valuer)
        return yield_valuer

    def create_aggregate_valuer(self, config, schema_field_name=None, inherit_valuers=None, aggregate_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        key_child_aggregate_valuers = []
        key_valuer = self.create_valuer(config["key_valuer"], schema_field_name=schema_field_name,
                                        inherit_valuers=inherit_valuers,
                                        aggregate_valuers=key_child_aggregate_valuers, **kwargs) \
            if "key_valuer" in config and config["key_valuer"] else None
        if key_child_aggregate_valuers:
            raise SyntaxError("aggregate conflict")

        calculate_child_aggregate_valuers = []
        calculate_inherit_valuers = []
        calculate_valuer = self.create_valuer(config["calculate_valuer"], schema_field_name=schema_field_name,
                                              inherit_valuers=calculate_inherit_valuers,
                                              aggregate_valuers=calculate_child_aggregate_valuers, **kwargs) \
            if "calculate_valuer" in config and config["calculate_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in calculate_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        manager = aggregate_valuers[0].get_manager() if aggregate_valuers else None
        aggregate_valuer = valuer_cls(key_valuer, calculate_valuer, current_inherit_valuers, manager, schema_field_name, None)
        if aggregate_valuers is not None:
            aggregate_valuers.append(aggregate_valuer)
        return aggregate_valuer

    def create_call_valuer(self, config, inherit_valuers=None, define_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        value_valuer = self.create_valuer(config["value_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "value_valuer" in config and config["value_valuer"] else None

        calculate_inherit_valuers = []
        calculate_valuer = self.create_valuer(config["calculate_valuer"], inherit_valuers=calculate_inherit_valuers,
                                              define_valuers=define_valuers, **kwargs) \
            if "calculate_valuer" in config and config["calculate_valuer"] else None

        current_inherit_valuers = []
        if not calculate_inherit_valuers:
            if define_valuers and config["key"] in define_valuers:
                calculate_valuer = define_valuers[config["key"]]
            else:
                define_valuers[config["key"]] = calculate_valuer
        else:
            for inherit_valuer in calculate_inherit_valuers:
                inherit_valuer["reflen"] -= 1
                if inherit_valuer["reflen"] == 0:
                    current_inherit_valuers.append(inherit_valuer["valuer"])
                elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                    inherit_valuers.append(inherit_valuer)

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers,
                                           define_valuers=define_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(value_valuer, calculate_valuer, return_valuer, current_inherit_valuers, None, config['key'], None)

    def create_assign_valuer(self, config, inherit_valuers=None, global_variables=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        calculate_inherit_valuers = []
        calculate_valuer = self.create_valuer(config["calculate_valuer"], inherit_valuers=calculate_inherit_valuers,
                                              global_variables=global_variables, **kwargs) \
            if "calculate_valuer" in config and config["calculate_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers,
                                           global_variables=global_variables, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        current_inherit_valuers = []
        for inherit_valuer in calculate_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(global_variables, calculate_valuer, return_valuer, current_inherit_valuers, config['key'], filter)

    def create_lambda_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        calculate_inherit_valuers = []
        calculate_valuer = self.create_valuer(config["calculate_valuer"], inherit_valuers=calculate_inherit_valuers,
                                              **kwargs) if "calculate_valuer" in config and config["calculate_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in calculate_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(calculate_valuer, current_inherit_valuers, config['key'], None)

    def create_foreach_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        value_valuer = self.create_valuer(config["value_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "value_valuer" in config and config["value_valuer"] else None

        calculate_inherit_valuers = []
        calculate_valuer = self.create_valuer(config["calculate_valuer"], inherit_valuers=calculate_inherit_valuers, **kwargs) \
            if "calculate_valuer" in config and config["calculate_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in calculate_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(value_valuer, calculate_valuer, return_valuer, current_inherit_valuers,
                          config['key'], None)

    def create_break_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(return_valuer, current_inherit_valuers, config['key'], None)

    def create_continue_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)
        return valuer_cls(return_valuer, current_inherit_valuers, config['key'], None)


    def create_if_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        if "value_valuer" in config and config["value_valuer"]:
            value_valuer = self.create_valuer(config["value_valuer"], inherit_valuers=inherit_valuers, **kwargs)
        else:
            value_valuer = None

        true_valuer = self.create_valuer(config["true_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "true_valuer" in config and config["true_valuer"] else None

        false_valuer = self.create_valuer(config["false_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "false_valuer" in config and config["false_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(true_valuer, false_valuer, value_valuer, return_valuer, current_inherit_valuers, config["key"], None)

    def create_match_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        if "value_valuer" in config and config["value_valuer"]:
            value_valuer = self.create_valuer(config["value_valuer"], inherit_valuers=inherit_valuers, **kwargs)
        else:
            value_valuer = None

        match_valuers, match_inherit_valuers = {}, []
        for key, valuer_config in config["match_valuers"].items():
            match_valuers[key] = self.create_valuer(valuer_config, inherit_valuers=match_inherit_valuers, **kwargs)

        default_match_valuer = self.create_valuer(config["default_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "default_valuer" in config and config["default_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        current_inherit_valuers = []
        for inherit_valuer in match_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(match_valuers, default_match_valuer, value_valuer, return_valuer, current_inherit_valuers, config["key"], None)

    def create_state_valuer(self, config, inherit_valuers=None, global_states=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")

        calculate_inherit_valuers = []
        calculate_valuer = self.create_valuer(config["calculate_valuer"], inherit_valuers=calculate_inherit_valuers,
                                              global_states=global_states, **kwargs) \
            if "calculate_valuer" in config and config["calculate_valuer"] else None

        default_inherit_valuers = []
        default_valuer = self.create_valuer(config["default_valuer"], inherit_valuers=default_inherit_valuers,
                                              global_states=global_states, **kwargs) \
            if "default_valuer" in config and config["default_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers,
                                           global_states=global_states, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        current_inherit_valuers = []
        for inherit_valuer in calculate_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        for inherit_valuer in default_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(global_states, calculate_valuer, default_valuer, return_valuer, current_inherit_valuers, config['key'], filter)

    def create_cache_valuer(self, config, inherit_valuers=None, **kwargs):
        valuer_cls = self.find_valuer_driver(config["name"])
        if not valuer_cls:
            raise ValuerUnknownException(config["name"] + " is unknown")
        if config["key"] not in self.tasker.caches:
            raise CacheUnknownException(config["key"] + " is unknown")

        key_valuer = self.create_valuer(config["key_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "key_valuer" in config and config["key_valuer"] else None

        calculate_valuer = self.create_valuer(config["calculate_valuer"], inherit_valuers=inherit_valuers, **kwargs) \
            if "calculate_valuer" in config and config["calculate_valuer"] else None

        return_inherit_valuers = []
        return_valuer = self.create_valuer(config["return_valuer"], inherit_valuers=return_inherit_valuers, **kwargs) \
            if "return_valuer" in config and config["return_valuer"] else None

        filter_cls = self.find_filter_driver(config["filter"]["name"]) if "filter" in config and config["filter"] else None
        filter = filter_cls(config["filter"]["args"]) if filter_cls else None

        current_inherit_valuers = []
        for inherit_valuer in return_inherit_valuers:
            inherit_valuer["reflen"] -= 1
            if inherit_valuer["reflen"] == 0:
                current_inherit_valuers.append(inherit_valuer["valuer"])
            elif inherit_valuer["reflen"] > 0 and inherit_valuers is not None:
                inherit_valuers.append(inherit_valuer)

        return valuer_cls(self.tasker.caches[config["key"]], key_valuer, calculate_valuer, return_valuer,
                                             current_inherit_valuers, config['key'], filter)