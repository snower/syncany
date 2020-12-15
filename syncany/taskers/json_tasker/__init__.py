# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import time
import copy
import logging.config
import json
from collections import OrderedDict
from ...logger import get_logger
from ..tasker import Tasker
from ...calculaters.import_calculater import create_import_calculater
from ...utils import get_expression_name
from .valuer_compiler import ValuerCompiler
from .valuer_creater import ValuerCreater
from .loader_creater import LoaderCreater
from .outputer_creater import OutputerCreater
from ...hook import PipelinesHooker
from ...errors import LoaderUnknownException, OutputerUnknownException, ValuerUnknownException, DatabaseUnknownException, CalculaterUnknownException

class JsonTasker(Tasker, ValuerCompiler, ValuerCreater, LoaderCreater, OutputerCreater):
    DEFAULT_CONFIG = {
        "name": "",
        "input": "",
        "output": "",
        "querys": {},
        "databases": [],
        "imports": {},
        "defines": {},
        "variables": {},
        "schema": {},
        "pipelines": [],
    }

    def __init__(self, json_filename):
        self.start_time = time.time()
        self.config = copy.deepcopy(self.DEFAULT_CONFIG)
        self.name = ""

        if isinstance(json_filename, dict):
            self.config.update(copy.deepcopy(json_filename))
            self.json_filename = "__inline__::" + json_filename.get("name", str(int(time.time())))
        else:
            self.json_filename = json_filename
        super(JsonTasker, self).__init__()

        self.join_loaders = {}

        self.valuer_compiler = {
            "const_valuer": self.compile_const_valuer,
            "db_valuer": self.compile_db_valuer,
            "inherit_valuer": self.compile_inherit_valuer,
            "db_join_valuer": self.compile_db_join_valuer,
            "case_valuer": self.compile_case_valuer,
            "calculate_valuer": self.compile_calculate_valuer,
            "schema_valuer": self.compile_schema_valuer,
            "make_valuer": self.compile_make_valuer,
            "let_valuer": self.compile_let_valuer,
            "yield_valuer": self.compile_yield_valuer,
            "aggregate_valuer": self.compile_aggregate_valuer,
            "call_valuer": self.compile_call_valuer,
            "assign_valuer": self.compile_assign_valuer,
        }

        self.valuer_creater = {
            "const_valuer": self.create_const_valuer,
            "db_valuer": self.create_db_valuer,
            "inherit_valuer": self.create_inherit_valuer,
            "db_join_valuer": self.create_db_join_valuer,
            "case_valuer": self.create_case_valuer,
            "calculate_valuer": self.create_calculate_valuer,
            "schema_valuer": self.create_schema_valuer,
            "make_valuer": self.create_make_valuer,
            "let_valuer": self.create_let_valuer,
            "yield_valuer": self.create_yield_valuer,
            "aggregate_valuer": self.create_aggregate_valuer,
            "call_valuer": self.create_call_valuer,
            "assign_valuer": self.create_assign_valuer,
        }

        self.loader_creater = {
            "const_loader": self.create_const_loader,
            "db_loader": self.create_db_loader,
            "db_join_loader": self.create_db_join_loader,
        }

        self.outputer_creater = {
            "db_update_delete_insert_outputer": self.create_db_update_delete_insert_outputer,
            "db_update_insert_outputer": self.create_db_update_insert_outputer,
            "db_delete_insert_outputer": self.create_db_delete_insert_outputer,
            "db_insert_outputer": self.create_db_insert_outputer,
        }

    def load_json(self, filename):
        if filename[:12] == "__inline__::":
            config, self.config = self.config, copy.deepcopy(self.DEFAULT_CONFIG)
            for k, v in self.DEFAULT_CONFIG.items():
                if k in config and not config[k]:
                    config.pop(k)
        else:
            with open(filename, "r") as fp:
                config = json.load(fp)

        extends = config.pop("extends") if "extends" in config else []
        if isinstance(extends, (tuple, set, list)):
            for json_filename in extends:
                self.load_json(json_filename)
        else:
            self.load_json(extends)

        for k, v in config.items():
            if k in ("imports", "defines", "variables", "logger"):
                if not isinstance(v, dict) or not isinstance(self.config.get(k, {}), dict):
                    continue

                if k not in self.config:
                    self.config[k] = v
                else:
                    self.config[k].update(v)
            elif k == "databases":
                if not isinstance(v, list) or not isinstance(self.config.get(k, []), list):
                    continue

                if k not in self.config:
                    self.config[k] = v
                else:
                    databases = {database["name"]: database for database in self.config[k]}
                    for database in v:
                        if database["name"] in databases:
                            databases[database["name"]].update(database)
                        else:
                            self.config[k].append(database)
            elif k == "pipelines":
                if self.config[k]:
                    pipelines = copy.copy(self.config[k] if isinstance(self.config[k], list)
                                            and not isinstance(self.config[k][0], str) else [self.config[k]])
                else:
                    pipelines = []
                if v:
                    for pipeline in (v if isinstance(v, list) and not isinstance(v[0], str) else [v]):
                        pipelines.append(pipeline)
                self.config[k] = pipelines
            else:
                self.config[k] = v

    def load_databases(self):
        for config in self.config["databases"]:
            database_cls = self.find_database_driver(config.pop("driver"))
            if not database_cls:
                raise DatabaseUnknownException(config["name"] + " is unknown")
            self.databases[config["name"]] = database_cls(config)

    def load_imports(self):
        for name, package in self.config["imports"].items():
            module = __import__(package, {}, {})
            try:
                if not self.find_calculater_driver(name):
                    self.register_calculater_driver(name, create_import_calculater(name, module))
            except CalculaterUnknownException:
                self.register_calculater_driver(name, create_import_calculater(name, module))

    def compile_logging(self):
        if "logger" in self.config and isinstance(self.config["logger"], dict):
            logging.config.dictConfig(self.config["logger"])

    def compile_filter_calculater(self, calculater):
        keys = calculater[0][1:].split("|")

        calculater_cls = self.find_calculater_driver(keys[0])
        if not calculater_cls:
            return calculater
        calculater_args = []
        for value in calculater[1:]:
            if isinstance(value, list) and value and value[0][0] == "@":
                calculater_args.append(self.compile_filter_calculater(value))
            else:
                calculater_args.append(value)
        value = calculater_cls(keys[0], *tuple(calculater_args)).calculate()

        filters = (keys[1] if len(keys) >= 2 else "str").split(" ")
        filters_args = (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None
        filter_cls = self.find_filter_driver(filters[0])
        if filter_cls:
            return filter_cls(filters_args).filter(value)
        return value

    def compile_filters_parse(self, config_querys):
        if isinstance(config_querys, str):
            keys = config_querys.split("|")
            exps = "=="
            filters = (keys[1] if len(keys) >= 2 else "str").split(" ")

            return [{
                "name": keys[0],
                "exps": exps,
                "type": filters[0],
                'type_args': (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None
            }]

        if isinstance(config_querys, dict):
            if len(config_querys) == 4 and "name" in config_querys and "exps" in config_querys \
                    and "type" in config_querys and "type_args" in config_querys:
                return [config_querys]

            querys = []
            for name, exps in config_querys.items():
                keys = name.split("|")
                filters = (keys[1] if len(keys) >= 2 else "str").split(" ")
                querys.append({
                    "name": keys[0],
                    "exps": exps,
                    "type": filters[0],
                    'type_args': (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None
                })
            return querys

        if isinstance(config_querys, list):
            querys = []
            for cq in config_querys:
                if isinstance(cq, str):
                    querys.extend(self.compile_filters_parse(cq))
                elif isinstance(cq, dict) and len(cq) == 4 and "name" in cq \
                    and "exps" in cq and "type" in cq and "type_args" in cq:
                    querys.extend(cq)
            return querys
        return []

    def compile_filters(self):
        self.config["querys"] = self.compile_filters_parse(self.config["querys"])

        arguments = []
        for filter in self.config["querys"]:
            if "exps" in filter:
                if isinstance(filter["exps"], str):
                    filter["exps"] = [filter["exps"]]

                if isinstance(filter["exps"], list):
                    for exp in filter["exps"]:
                        exp_name = get_expression_name(exp)
                        filter_cls = self.find_filter_driver(filter["type"])
                        if filter_cls is None:
                            filter_cls = self.find_filter_driver('str')
                        arguments.append({"name": '%s__%s' % (filter["name"], exp_name), "type": filter_cls(filter.get("type_args")),
                                          "help": "%s %s" % (filter["name"], exp)})
                elif isinstance(filter["exps"], dict):
                    for exp, value in filter["exps"].items():
                        exp_name = get_expression_name(exp)
                        filter_cls = self.find_filter_driver(filter["type"])
                        if filter_cls is None:
                            filter_cls = self.find_filter_driver('str')
                        if isinstance(value, list) and value and value[0][0] == "@":
                            value = self.compile_filter_calculater(value)
                        arguments.append({"name": '%s__%s' % (filter["name"], exp_name), "type": filter_cls(filter.get("type_args")),
                             "default": value, "help": "%s %s (default: %s)" % (filter["name"], exp, value)})
            else:
                filter_cls = self.find_filter_driver(filter["type"])
                if filter_cls is None:
                    filter_cls = self.find_filter_driver('str')
                arguments.append({"name": filter["name"], "type": filter_cls(filter.get("type_args")), "help": "%s" % filter["name"]})

        if "input" in self.config:
            if isinstance(self.config["input"], (list, tuple)) and self.config["input"] and self.config["input"][0][0] == "@":
                self.config["input"] = self.compile_filter_calculater(self.config["input"])
            if self.config["input"][:2] == "<<":
                arguments.append({"name": "@input", "type": str, "default": self.config["input"][2:],
                                  "help": "data input (default: %s)" % self.config["input"][2:]})

        if "loader" in self.config:
            if isinstance(self.config["loader"], (list, tuple)) and self.config["loader"] and self.config["loader"][0][0] == "@":
                self.config["loader"] = self.compile_filter_calculater(self.config["loader"])
            if self.config["loader"][:2] == "<<":
                arguments.append({"name": "@loader", "type": str, "default": self.config["loader"][2:],
                                  "choices": ("db_loader",),
                                  "help": "data loader (default: %s)" % self.config["loader"][2:]})

        if "output" in self.config:
            if isinstance(self.config["output"], (list, tuple)) and self.config["output"] and self.config["output"][0][0] == "@":
                self.config["output"] = self.compile_filter_calculater(self.config["output"])
            if self.config["output"][:2] == ">>":
                arguments.append({"name": "@output", "type": str, "default": self.config["output"][2:],
                                  "help": "data output (default: %s)" % self.config["output"][2:]})

        if "outputer" in self.config:
            if isinstance(self.config["outputer"], (list, tuple)) and self.config["outputer"] and self.config["outputer"][0][0] == "@":
                self.config["outputer"] = self.compile_filter_calculater(self.config["outputer"])
            if self.config["outputer"][:2] == ">>":
                arguments.append({"name": "@outputer", "type": str, "default": self.config["outputer"][2:],
                                  "choices": tuple(self.outputer_creater.keys()),
                                  "help": "data outputer (default: %s)" % self.config["outputer"][2:]})

        arguments.append({"name": "@batch", "type": int, "default": 0, "help": "per sync batch count (default: 0 all)"})
        return arguments

    def compile_key(self, key):
        if not isinstance(key, str) or key == "":
            return {"instance": None, "key": "", "inherit_reflen": 0, "value": key, "filter": None}

        if key[0] not in ("&", "$", "@", "|", "#"):
            return {"instance": None, "key": "", "inherit_reflen": 0, "value": key, "filter": None}

        inherit_reflen = 0
        if key[0] in ("&", "$"):
            tokens = key.split(".")
            instance = tokens[0][0]
            key = ".".join(tokens[1:])
            if key == "" and tokens[0] == "$":
                key = "*"
            if instance == "$" and tokens[0][1:] == ("$" * len(tokens[0][1:])):
                inherit_reflen = len(tokens[0][1:])
        else:
            instance = key[0]
            key = key[1:]

        key_filters = key.split("|")
        key = key_filters[0]

        filter, filter_args = None, None
        if len(key_filters) >= 2:
            filters = key_filters[1].split(" ")
            filter = filters[0]
            if len(filters) >= 2:
                filter_args = " ".join(filters[1:]) + "|".join(key_filters[2:])

        return {
            "instance": instance,
            "key": key,
            "inherit_reflen": inherit_reflen,
            'value': None,
            "filter": {
                "name": filter,
                "args": filter_args
            },
        }

    def compile_foreign_key(self, foreign_key):
        if isinstance(foreign_key, (list, tuple, set)):
            if not foreign_key or foreign_key[0][0] != "&":
                return None
            foreign_key, foreign_filter_configs = foreign_key[0], (foreign_key[1] if len(foreign_key) >= 2 else {})
        elif not foreign_key or foreign_key[0] != "&":
            return None
        else:
            foreign_filter_configs = {}

        foreign_key = ".".join(foreign_key.split(".")[1:])
        foreign_key = foreign_key.split("::")

        foreign_filters = []
        if isinstance(foreign_filter_configs, dict):
            for key, exps in foreign_filter_configs.items():
                if isinstance(exps, dict):
                    for exp, value in exps.items():
                        try:
                            exp = get_expression_name(exp)
                            foreign_filters.append((key, exp, value))
                        except KeyError: pass
                else:
                    foreign_filters.append((key, 'eq', exps))

        return {
            "database": foreign_key[0],
            "foreign_key": foreign_key[1],
            "foreign_filters": foreign_filters,
        }

    def compile_schema(self):
        if isinstance(self.config["schema"], str):
            if self.config["schema"] == "$.*":
                self.schema = ".*"
        else:
            self.schema = OrderedDict()
            schema, order_names = {}, [''] * len(self.config["schema"])
            for name, field in self.config["schema"].items():
                if name[0] == "$":
                    names = name.split(":")
                    name = ":".join(names[1:])
                    try:
                        index = names[0][2:]
                    except:
                        self.schema[name] = self.compile_valuer(field)
                        continue

                    order_names[index] = name
                    schema[name] = self.compile_valuer(field)
                else:
                    self.schema[name] = self.compile_valuer(field)

            for name in order_names:
                if name:
                    self.schema[name] = schema[name]

    def compile_valuer(self, valuer):
        if isinstance(valuer, dict):
            if "name" not in valuer or not valuer["name"].endswith("_valuer"):
                if "#case" not in valuer:
                    return self.compile_const_valuer(valuer)

                case_case = valuer.pop("#case")
                cases = {}
                case_default = valuer.pop("#end") if "#end" in valuer else None
                case_return = valuer.pop(":") if ":" in valuer else None
                for case_key, case_value in valuer.items():
                    if case_key and isinstance(case_key, str) and case_key[0] == ":" and case_key[1:].isdigit():
                        cases[int(case_key[1:])] = case_value
                    else:
                        cases[case_key] = case_value
                return self.compile_case_valuer('', None, case_case, cases, case_default, case_return)
            return valuer

        if isinstance(valuer, (list, tuple, set)):
            if not valuer:
                return self.compile_const_valuer(valuer)

            key = self.compile_key(valuer[0])
            if key["instance"] is None:
                return self.compile_const_valuer(valuer)

            if key["instance"] == "$":
                if len(valuer) not in (2, 3):
                    if "inherit_reflen" in key and key["inherit_reflen"] > 0:
                        return self.compile_inherit_valuer(key["key"], key["filter"], key["inherit_reflen"])
                    return self.compile_db_valuer(key["key"], key["filter"])

                foreign_key = self.compile_foreign_key(valuer[1])
                if foreign_key is None:
                    return self.compile_const_valuer(valuer)

                loader = {"name": "db_join_loader", "database": foreign_key["database"]}
                return self.compile_db_join_valuer(key["key"], loader, foreign_key["foreign_key"], foreign_key["foreign_filters"],
                                                   None, valuer[0], valuer[2] if len(valuer) >= 3 else None)

            if key["instance"] == "@":
                return self.compile_calculate_valuer(key["key"], key["filter"], valuer[1:])

            if key["instance"] == "#":
                if key["key"] == "const":
                    return self.compile_const_valuer(valuer[1:] if len(valuer) > 2 else (valuer[1] if len(valuer) > 1 else None))
                if key["key"] == "case" and len(valuer) in (2, 3, 4):
                    return self.compile_case_valuer(key["key"], key["filter"], None, valuer[1:], None)
                if key["key"] == "make" and len(valuer) in (2, 3, 4, 5):
                    return self.compile_make_valuer(key["key"], key["filter"], valuer[1], valuer[2:])
                if key["key"] == "let" and len(valuer) in (2, 3):
                    return self.compile_let_valuer(key["key"], key["filter"], valuer[1], valuer[2] if len(valuer) >= 3 else None)
                if key["key"] == "yield" and len(valuer) in (1, 2, 3):
                    return self.compile_yield_valuer(key["key"], key["filter"], valuer[1] if len(valuer) >= 1 else None,
                                                     valuer[2] if len(valuer) >= 3 else None)
                if key["key"] == "aggregate" and len(valuer) >= 3:
                    return self.compile_aggregate_valuer(key["key"], key["filter"], valuer[1], valuer[2] if len(valuer) == 3 else None,
                                                         None if len(valuer) == 3 else valuer[2:])
                if key["key"] == "call" and len(valuer) in (2, 3) and valuer[1] in self.config["defines"]:
                    return self.compile_call_valuer(valuer[1], key["filter"], valuer[2] if len(valuer) >= 3 else None,
                                                    self.config["defines"][valuer[1]])
                if key["key"] == "assign" and len(valuer) in (2, 3, 4):
                    return self.compile_assign_valuer(valuer[1], key["filter"], valuer[2] if len(valuer) >= 3 else None,
                                                    valuer[3] if len(valuer) >= 4 else None)

            return self.compile_const_valuer(valuer)

        key = self.compile_key(valuer)
        if key["instance"] is None:
            return self.compile_const_valuer(key["value"])

        if key["instance"] == "$":
            if "inherit_reflen" in key and key["inherit_reflen"] > 0:
                return self.compile_inherit_valuer(key["key"], key["filter"], key["inherit_reflen"])
            return self.compile_db_valuer(key["key"], key["filter"])

        if key["instance"] == "@":
            return self.compile_calculate_valuer(key["key"], key["filter"], [])

        if key["instance"] == "#":
            if key["key"] == "const":
                return self.compile_const_valuer(None)
            if key["key"] == "yield":
                return self.compile_yield_valuer(key["key"], key["filter"], None, None)
        return self.compile_const_valuer(valuer)

    def compile_pipelines(self):
        if not self.config["pipelines"]:
            return

        current_type = "compiled_valuers"
        valuers = {"compiled_valuers": [], "queried_valuers": [], "loaded_valuers": [], "outputed_valuers": []}
        for pipeline in self.config["pipelines"]:
            if isinstance(pipeline, list):
                if pipeline[0][:1] not in (">", "@"):
                    continue

                if pipeline[0][:3] == ">>>":
                    current_type, pipeline[0] = "outputed_valuers", pipeline[0][3:]
                elif pipeline[0][:2] == ">>":
                    current_type, pipeline[0] = "loaded_valuers", pipeline[0][2:]
                elif pipeline[0][:1] == ">":
                    current_type, pipeline[0] = "queried_valuers", pipeline[0][1:]

                if not pipeline[0]:
                    continue
            else:
                if pipeline[:1] not in (">", "@"):
                    continue

                if pipeline[:3] == ">>>":
                    current_type, pipeline = "outputed_valuers", pipeline[3:]
                elif pipeline[:2] == ">>":
                    current_type, pipeline = "loaded_valuers", pipeline[2:]
                elif pipeline[:1] == ">":
                    current_type, pipeline = "queried_valuers", pipeline[1:]

                if not pipeline:
                    continue

            valuer = self.create_valuer(self.compile_valuer(pipeline), define_valuers={},
                                        global_variables=dict(**self.config["variables"]))
            valuers[current_type].append(valuer)

        pipelines_hooker = PipelinesHooker(**valuers)
        self.add_hooker(pipelines_hooker)

    def create_valuer(self, config, **kwargs):
        if "name" not in config or not config["name"]:
            raise ValuerUnknownException(config["name"] + " is unknown")

        if config["name"] not in self.valuer_creater:
            valuer_cls = self.find_valuer_driver(config["name"])
            if not valuer_cls:
                raise ValuerUnknownException(config["name"] + " is unknown")
            config = {key: value for key, value in config.items() if key != "name"}
            return valuer_cls(**config)

        return self.valuer_creater[config["name"]](config, **kwargs)

    def create_loader(self, config, primary_keys):
        if "name" not in config or not config["name"]:
            raise LoaderUnknownException(config["name"] + " is unknown")

        if config["name"] not in self.loader_creater:
            loader_cls = self.find_loader_driver(config["name"])
            if not loader_cls:
                raise LoaderUnknownException(config["name"] + " is unknown")
            config = {key: value for key, value in config.items() if key != "name"}
            return loader_cls(**config)

        return self.loader_creater[config["name"]](config, primary_keys)

    def create_outputer(self, config, primary_keys):
        if "name" not in config or not config["name"]:
            raise OutputerUnknownException(config["name"] + " is unknown")

        if config["name"] not in self.outputer_creater:
            outputer_cls = self.find_outputer_driver(config["name"])
            if not outputer_cls:
                raise OutputerUnknownException(config["name"] + " is unknown")
            config = {key: value for key, value in config.items() if key != "name"}
            return outputer_cls(**config)

        return self.outputer_creater[config["name"]](config, primary_keys)

    def compile_loader(self):
        if self.config["input"][:2] == "<<":
            self.config["input"] = self.arguments.get("@input", self.config["input"][2:])
        if " use " in self.config["input"]:
            input_info = self.config["input"].split(" use ")
            if len(input_info) == 2:
                self.config["input"], self.config["loader"] = input_info[0].strip(), ("db_" + input_info[1] + "_loader").strip()
        input_loader = self.compile_foreign_key(self.config["input"])
        if not input_loader:
            raise LoaderUnknownException(self.config["input"] + "is not define")
        db_name = input_loader["database"].split(".")[0]

        if "loader" in self.config and self.config["loader"][:2] == "<<" and "@loader" in self.arguments:
            self.config["loader"] = self.arguments["@loader"]
        try:
            loader = self.config.get("loader", self.databases[db_name].get_default_loader())
        except KeyError:
            raise DatabaseUnknownException(db_name + " is unknown")
        loader_config = {
            "name": loader,
            "database": input_loader["database"],
            "is_yield": False,
        }
        self.loader = self.create_loader(loader_config, [input_loader["foreign_key"]])

        if isinstance(self.schema, dict):
            for name, valuer in self.schema.items():
                inherit_valuers, yield_valuers, aggregate_valuers = [], [], []
                valuer = self.create_valuer(valuer, schema_field_name=name, inherit_valuers=inherit_valuers,
                                            join_loaders=self.join_loaders, yield_valuers=yield_valuers,
                                            aggregate_valuers=aggregate_valuers, define_valuers={},
                                            global_variables=dict(**self.config["variables"]))
                if valuer:
                    self.loader.add_valuer(name, valuer)
                if inherit_valuers:
                    raise OverflowError(name + " inherit out of range")
                if yield_valuers or aggregate_valuers:
                    loader_config["is_yield"] = True
                    self.loader.is_yield = True
        elif self.schema == ".*":
            self.loader.add_key_matcher(".*", self.create_valuer(self.compile_db_valuer("", None)))

        for filter in self.config["querys"]:
            filter_name = filter["name"]
            if "exps" in filter:
                if isinstance(filter["exps"], str):
                    exps = [filter["exps"]]
                else:
                    exps = filter["exps"]

                for exp in exps:
                    exp_name = get_expression_name(exp)
                    if hasattr(self.loader, "filter_%s" % exp_name) and "%s__%s" % (filter_name, exp_name) in self.arguments:
                        getattr(self.loader, "filter_%s" % exp_name)(filter_name, self.arguments["%s__%s" % (filter_name, exp_name)])
            else:
                if hasattr(self.loader, "filter_eq") and filter_name in self.arguments:
                    getattr(self.loader, "filter_eq")(filter_name, self.arguments[filter_name])

    def compile_outputer(self):
        if self.config["output"][:2] == ">>":
            self.config["output"] = self.arguments.get("@output", self.config["output"][2:])
        if " use " in self.config["output"]:
            output_info = self.config["output"].split(" use ")
            if len(output_info) == 2:
                short_names = {"I": "insert", "UI": "update_insert", "UDI": "update_delete_insert", "DI": "delete_insert"}
                if output_info[1] in short_names:
                    output_info[1] = short_names[output_info[1]]
                self.config["output"], self.config["outputer"] = output_info[0].strip(), ("db_" + output_info[1] + "_outputer").strip()
        output_outputer = self.compile_foreign_key(self.config["output"])
        if not output_outputer:
            raise OutputerUnknownException(self.config["output"] + "is not define")
        db_name = output_outputer["database"].split(".")[0]

        if "outputer" in self.config and self.config["outputer"][:2] == ">>" and "@outputer" in self.arguments:
            self.config["outputer"] = self.arguments["@outputer"]
        try:
            outputer = self.config.get("outputer", self.databases[db_name].get_default_outputer())
        except KeyError:
            raise DatabaseUnknownException(db_name + " is unknown")
        outputer_config = {
            "name": outputer,
            "database": output_outputer["database"],
        }
        self.outputer = self.create_outputer(outputer_config, [output_outputer["foreign_key"]])

        if isinstance(self.schema, dict):
            for name, valuer in self.schema.items():
                valuer = self.create_valuer(self.compile_db_valuer(name, None))
                if valuer:
                    if name in self.loader.schema:
                        valuer.filter = self.loader.schema[name].get_final_filter()
                    self.outputer.add_valuer(name, valuer)

        for filter in self.config["querys"]:
            filter_name = filter["name"]
            value_filter = lambda v: v
            if filter_name not in self.outputer.schema:
                for field_name, valuer in self.outputer.schema.items():
                    fields = valuer.get_fields()
                    if filter_name in fields:
                        if not valuer.childs():
                            filter_name = field_name
                            if valuer.filter:
                                value_filter = valuer.filter.filter
                        break

                if filter_name == filter["name"]:
                    continue
            else:
                valuer = self.outputer.schema[filter_name]
                if valuer.filter:
                    value_filter = valuer.filter.filter


            if "exps" in filter:
                if isinstance(filter["exps"], str):
                    exps = [filter["exps"]]
                else:
                    exps = filter["exps"]

                for exp in exps:
                    exp_name = get_expression_name(exp)
                    if hasattr(self.outputer, "filter_%s" % exp_name) and "%s__%s" % (filter_name, exp_name) in self.arguments:
                        value = value_filter(self.arguments["%s__%s" % (filter_name, exp_name)])
                        getattr(self.outputer, "filter_%s" % exp_name)(filter_name, value)
            else:
                if hasattr(self.outputer, "filter_eq") and filter_name in self.arguments:
                    value = value_filter(self.arguments[filter_name])
                    getattr(self.outputer, "filter_eq")(filter_name, value)

    def print_statistics(self, loader_name, loader_statistics, outputer_name, outputer_statistics, join_loader_count, join_loader_statistics):
        statistics = ["loader_%s: %s" % (key, value) for key, value in loader_statistics.items()]
        get_logger().info("%s loader: %s <- %s %s", self.name, loader_name, self.input, " ".join(statistics))

        get_logger().info("%s join_count: %s %s", self.name, join_loader_count, " ".join(["join_%s: %s" % (key, value) for key, value in join_loader_statistics.items()]))

        statistics = ["outputer_%s: %s" % (key, value) for key, value in outputer_statistics.items()]
        get_logger().info("%s outputer: %s -> %s %s", self.name, outputer_name, self.output, " ".join(statistics))

    def merge_statistics(self, loader_statistics, outputer_statistics, join_loaders_statistics, loader, outputer, join_loaders):
        for total_statistics, statisticers in ((loader_statistics, [loader]), (outputer_statistics, [outputer]), (join_loaders_statistics, join_loaders)):
            for statisticer in statisticers:
                for key, value in statisticer.statistics().items():
                    if key not in total_statistics:
                        total_statistics[key] = value
                    elif isinstance(value, (int, float)):
                        total_statistics[key] += value
                    else:
                        total_statistics[key] = value

        return loader.__class__.__name__, loader_statistics, outputer.__class__.__name__, outputer_statistics, len(join_loaders), join_loaders_statistics

    def get_dependency(self):
        if "dependency" not in self.config:
            return []

        if isinstance(self.config["dependency"], (tuple, set, list)):
            return self.config["dependency"]
        return [self.config["dependency"]]

    def load(self):
        super(JsonTasker, self).load()
        self.load_json(self.json_filename)
        self.name = self.config["name"]
        self.compile_logging()
        self.load_imports()
        return self.compile_filters()

    def compile(self, arguments):
        super(JsonTasker, self).compile(arguments)

        self.load_databases()
        self.compile_schema()
        self.compile_pipelines()
        self.compile_loader()
        self.compile_outputer()
        self.input = self.config["input"]
        self.output = self.config["output"]
        for hooker in self.hookers:
            hooker.compiled(self)

    def run_batch(self, batch_count):
        batch_index = 0
        loader_statistics = {}
        outputer_statistics = {}
        join_loaders_statistics = {}

        cursor_data, ocursor_data = None, None
        get_logger().info("%s start %s -> %s batch cursor: %s", self.name, 1, batch_count, "")

        while True:
            batch_index += 1
            loader = self.loader.clone()
            outputer = self.outputer.clone()
            self.join_loaders = {key: join_loader.clone() for key, join_loader in self.join_loaders.items()}

            if cursor_data:
                vcursor = []
                for primary_key in loader.primary_keys:
                    cv = cursor_data.get(primary_key, '')
                    loader.filter_gt(primary_key, cv)
                    vcursor.append("%s -> %s" % (primary_key, cv))

                get_logger().info("%s start %s -> %s batch cursor: %s", self.name, batch_index, batch_count,
                                  " ".join(vcursor))

            loader.filter_limit(batch_count)
            loader.load()
            for hooker in self.hookers:
                loader.datas = hooker.queried(self, loader.datas)

            datas = loader.get()
            if not datas:
                break
            for hooker in self.hookers:
                datas = hooker.loaded(self, datas)

            cursor_data = loader.last_data
            if ocursor_data:
                for primary_key in outputer.primary_keys:
                    outputer.filter_gt(primary_key, ocursor_data.get(primary_key, ''))
            outputer.store(datas)
            ocursor_data = datas[-1]
            for hooker in self.hookers:
                hooker.outputed(self, datas)

            self.print_statistics(*self.merge_statistics({}, {}, {}, loader, outputer, self.join_loaders.values()))
            self.merge_statistics(loader_statistics, outputer_statistics, join_loaders_statistics, loader,
                                  outputer, self.join_loaders.values())

        get_logger().info("%s end %s -> %s batch show statistics", self.name, batch_index - 1, batch_count)
        statistics = (self.loader.__class__.__name__, loader_statistics, self.outputer.__class__.__name__, outputer_statistics,
                      len(self.join_loaders), join_loaders_statistics)
        self.print_statistics(*statistics)
        return statistics

    def run(self):
        batch_count = int(self.arguments.get("@batch", 0))
        try:
            if batch_count > 0:
                statistics = self.run_batch(batch_count)
            else:
                self.loader.load()
                for hooker in self.hookers:
                    self.loader.datas = hooker.queried(self, self.loader.datas)

                datas = self.loader.get()
                if datas:
                    for hooker in self.hookers:
                        datas = hooker.loaded(self, datas)

                    self.outputer.store(datas)
                    for hooker in self.hookers:
                        hooker.outputed(self, datas)

                statistics = self.merge_statistics({}, {}, {}, self.loader, self.outputer, self.join_loaders.values())
                self.print_statistics(*statistics)
        finally:
            for name, database in self.databases.items():
                database.close()

        get_logger().info("%s finish %s %s %.2fms", self.name, self.json_filename, self.config.get("name"), (time.time() - self.start_time) * 1000)
        return statistics