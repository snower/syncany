# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import time
import datetime
import pytz
import copy
import types
import logging.config
from ...logger import get_logger
from ..tasker import Tasker
from ..config import load_config
from .states import States
from .option import DataValuerOutputerOption
from ...calculaters.import_calculater import create_import_calculater
from ...utils import get_expression_name, gen_runner_id
from .valuer_compiler import ValuerCompiler
from .valuer_creater import ValuerCreater
from .loader_creater import LoaderCreater
from .outputer_creater import OutputerCreater
from ...loaders.cache import CacheLoader
from ...hook import PipelinesHooker
from ...errors import LoaderUnknownException, OutputerUnknownException, \
    ValuerUnknownException, DatabaseUnknownException, CalculaterUnknownException, \
    CacheUnknownException, SourceUnknownException

class CoreTasker(Tasker):
    DEFAULT_CONFIG = {
        "name": "",
        "input": "",
        "output": "",
        "arguments": {},
        "querys": [],
        "databases": [],
        "caches": [],
        "imports": {},
        "sources": {},
        "defines": {},
        "variables": {},
        "schema": {},
        "pipelines": [],
        "options": {},
        "dependencys": [],
        "states": [],
    }

    def __init__(self, config_filename, *args, **kwargs):
        super(CoreTasker, self).__init__(*args, **kwargs)
        self.closed = False
        self.terminated = False
        self.states = States()
        self.valuer_compiler = ValuerCompiler(self)
        self.valuer_creater = ValuerCreater(self)
        self.loader_creater = LoaderCreater(self)
        self.outputer_creater = OutputerCreater(self)
        self.config = copy.deepcopy(self.DEFAULT_CONFIG)
        self.name = ""

        if isinstance(config_filename, dict):
            self.config.update(copy.deepcopy(config_filename))
            self.config_filename = "__inline__::" + config_filename.get("name", str(int(time.time())))
        else:
            self.config_filename = config_filename
        self.join_loaders = {}
        self.global_variables = {}
        self.batch_cursor = None

    def load_config(self, filename):
        if filename[:12] == "__inline__::":
            config, self.config = self.config, copy.deepcopy(self.DEFAULT_CONFIG)
            for k, v in self.DEFAULT_CONFIG.items():
                if k in config and not config[k]:
                    config.pop(k)
        else:
            config = load_config(filename)

        extends = config.pop("extends") if "extends" in config else []
        if isinstance(extends, list):
            for config_filename in extends:
                self.load_config(config_filename)
        else:
            self.load_config(extends)

        for k, v in config.items():
            if k in ("arguments", "imports", "defines", "variables", "sources", "logger", "options"):
                if not isinstance(v, dict) or not isinstance(self.config.get(k, {}), dict):
                    continue

                if k not in self.config:
                    self.config[k] = v
                else:
                    self.config[k].update(v)
            elif k in ("databases", "caches"):
                if not isinstance(v, list) or not isinstance(self.config.get(k, []), list):
                    continue

                if k not in self.config:
                    self.config[k] = v
                else:
                    vs = {c["name"]: c for c in self.config[k]}
                    for d in v:
                        if d["name"] in vs:
                            vs[d["name"]].update(d)
                        else:
                            self.config[k].append(d)
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
            elif k == "states":
                if self.config[k]:
                    self.config[k].extend(v if isinstance(v, list) else [v])
                else:
                    self.config[k] = v if isinstance(v, list) else [v]
            else:
                self.config[k] = v

    def load_databases(self):
        for config in self.config["databases"]:
            config = dict(**config)
            database_cls = self.find_database_driver(config.pop("driver"))
            if not database_cls:
                raise DatabaseUnknownException(config["name"] + " is unknown")
            self.databases[config["name"]] = database_cls(self.manager.database_manager, config)

    def load_caches(self):
        for config in self.config["caches"]:
            config = dict(**config)
            if config["database"] not in self.databases:
                continue
            name = config.pop("name")
            try:
                self.caches[name] = CacheLoader(name, self.databases[config.pop("database")], config)
            except NotImplementedError:
                raise CacheUnknownException(name + " is unknown")

    def load_imports(self):
        for name, package in self.config["imports"].items():
            module = __import__(package, {}, {})
            try:
                if not self.find_calculater_driver(name):
                    self.register_calculater_driver(name, create_import_calculater(name, module))
            except CalculaterUnknownException:
                self.register_calculater_driver(name, create_import_calculater(name, module))

    def load_sources(self):
        for name, filename in list(self.config["sources"].items()):
            try:
                self.config["sources"][name] = load_config(filename)
            except Exception as e:
                raise SourceUnknownException("%s(%s)" % (filename, str(e)))

    def load_states(self):
        for state in self.config["states"]:
            self.states.add_tasker(CoreTasker(state, self.manager, self))
        self.states.compile(self)
        self.states.load(self)

    def load_cursor(self):
        if "@recovery" not in self.arguments or not self.arguments["@recovery"]:
            return
        if "cursor" not in self.config or self.config["cursor"]:
            if "@cursor" in self.states:
                if len(self.loader.primary_keys) > 1:
                    if isinstance(self.states["@cursor"], dict):
                        self.batch_cursor = self.states["@cursor"]
                    else:
                        self.batch_cursor = {[self.loader.primary_keys[0]]: self.states["@cursor"]}
                else:
                    self.batch_cursor = {[self.loader.primary_keys[0]]: self.states["@cursor"]}
            return
        self.batch_cursor = {}
        if len(self.loader.primary_keys) > 1:
            if isinstance(self.config["cursor"], dict):
                for key, value in self.config["cursor"].items():
                    self.batch_cursor[key] = self.run_valuer(value, {})
            else:
                self.batch_cursor = {[self.loader.primary_keys[0]]: self.config["cursor"]}
        else:
            self.batch_cursor[self.loader.primary_keys[0]] = self.run_valuer(self.config["cursor"], {})

    def config_logging(self):
        if "logger" in self.config and isinstance(self.config["logger"], dict):
            logging.config.dictConfig(self.config["logger"])
        else:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s %(process)d %(levelname)s %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')

    def compile_variables(self):
        for key, value in self.config["variables"].items():
            self.global_variables[key] = self.run_valuer(value, {})

    def compile_sources(self, config=None):
        if isinstance(config, dict):
            for key, value in list(config.items()):
                if isinstance(value, str):
                    if value[:1] == "%" and value[1:] in self.config["sources"]:
                        config[key] = copy.deepcopy(self.config["sources"][value[1:]])
                        if isinstance(config[key], (dict, list)):
                            self.compile_sources(config[key])
                    elif value[:1] == "?":
                        name = value[1:].split("|")
                        if name[0] in self.arguments:
                            config[key] = self.arguments[name[0]]
                    elif value[:2] in ("\\%", "\\?", "\\\\"):
                        config[key] = value[1:]
                elif isinstance(value, (dict, list)):
                    self.compile_sources(value)
        elif isinstance(config, list):
            for i in range(len(config)):
                value = config[i]
                if isinstance(value, str):
                    if value[:1] == "%" and value[1:] in self.config["sources"]:
                        config[i] = copy.deepcopy(self.config["sources"][value[1:]])
                        if isinstance(config[i], (dict, list)):
                            self.compile_sources(config[i])
                    elif value[:1] == "?":
                        name = value[1:].split("|")
                        if name[0] in self.arguments:
                            config[i] = self.arguments[name[0]]
                    elif value[:2] in ("\\%", "\\?", "\\\\"):
                        config[i] = value[1:]
                elif isinstance(value, (dict, list)):
                    self.compile_sources(value)

    def compile_options(self):
        if "schema" not in self.config["options"]:
            self.config["options"]["schema"] = {}

        if isinstance(self.config["schema"], dict):
            for key, valuer in tuple(self.config["schema"].items()):
                if key[-1:] != "?":
                    continue
                self.config["options"]["schema"][key[:-1]] = {"chaned_require_update": True}
                self.config["schema"][key[:-1]] = valuer
                self.config["schema"].pop(key)

    def compile_filters_parse_exps(self, exps):
        if isinstance(exps, dict):
            result = {}
            for exp, value in exps.items():
                try:
                    exp_name = get_expression_name(exp)
                except KeyError:
                    continue
                result[exp_name] = value
            return result if result else None
        if isinstance(exps, list):
            result = []
            for exp in exps:
                try:
                    exp_name = get_expression_name(exp)
                except KeyError:
                    continue
                result.append(exp_name)
            return result if result else None
        return {"eq": exps}

    def compile_filters_parse(self, config_querys):
        if isinstance(config_querys, str):
            keys = config_querys.split("|")
            filters = (keys[1] if len(keys) >= 2 else "str").split(" ")
            return [{
                "name": keys[0],
                "exps": ["eq"],
                "type": filters[0],
                'type_args': (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None,
                'refs': {}
            }]

        if isinstance(config_querys, dict):
            if len(config_querys) == 4 and "name" in config_querys and "exps" in config_querys \
                    and "type" in config_querys and "type_args" in config_querys:
                exps = self.compile_filters_parse_exps(config_querys["exps"])
                if not exps:
                    return []
                config_querys["exps"] = exps
                config_querys["refs"] = {}
                return [config_querys]

            querys = []
            for name, exps in config_querys.items():
                keys = name.split("|")
                exps = self.compile_filters_parse_exps(exps)
                if not exps:
                    continue
                filters = (keys[1] if len(keys) >= 2 else "str").split(" ")
                querys.append({
                    "name": keys[0],
                    "exps": exps,
                    "type": filters[0],
                    'type_args': (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None,
                    'refs': {}
                })
            return querys

        if isinstance(config_querys, list):
            querys = []
            for cq in config_querys:
                if isinstance(cq, str):
                    querys.extend(self.compile_filters_parse(cq))
                elif isinstance(cq, dict) and len(cq) == 4 and "name" in cq \
                    and "exps" in cq and "type" in cq and "type_args" in cq:
                    exps = self.compile_filters_parse_exps(cq["exps"])
                    if not exps:
                        continue
                    cq["exps"] = exps
                    cq["refs"] = {}
                    querys.append(cq)
            return querys
        return []

    def compile_merge_argument(self, argument, child_argument):
        for k, v in child_argument.items():
            if k == "name":
                continue
            if k != "type" and k in argument:
                continue
            argument[k] = v
        return argument

    def compile_filters_arguments(self, arguments_names, arguments):
        self.config["querys"] = self.compile_filters_parse(self.config["querys"])

        for query in self.config["querys"]:
            if isinstance(query["exps"], list):
                for exp_name in query["exps"]:
                    filter_cls = self.find_filter_driver(query["type"])
                    if filter_cls is None:
                        filter_cls = self.find_filter_driver('str')

                    if exp_name == "eq":
                        argument = {"name": '%s' % query["name"], "type": filter_cls(query.get("type_args")),
                                    "help": "query argument '%s'" % query["name"]}
                    elif exp_name == "in":
                        argument = {"name": '%s' % query["name"], "type": filter_cls(query.get("type_args")),
                                    "nargs": "+", "action": "extend",
                                    "help": "query arguments '%s'" % query["name"]}
                    else:
                        argument = {"name": '%s__%s' % (query["name"], exp_name), "type": filter_cls(query.get("type_args")),
                                    "help": "query argument '%s__%s'" % (query["name"], exp_name)}
                    if argument["name"] in arguments_names:
                        self.compile_merge_argument(arguments_names[argument["name"]], argument)
                        continue
                    arguments.append(argument)
                    arguments_names[argument["name"]] = argument
            elif isinstance(query["exps"], dict):
                for exp_name, exp_value in query["exps"].items():
                    if exp_value[:1] == "?":
                        query["refs"][exp_name] = exp_value[1:]
                        continue
                    filter_cls = self.find_filter_driver(query["type"])
                    if filter_cls is None:
                        filter_cls = self.find_filter_driver('str')
                    exp_value = self.run_valuer(exp_value, {})

                    if exp_name == "eq":
                        argument = {"name": '%s' % query["name"], "type": filter_cls(query.get("type_args")),
                                    "default": exp_value, "help": "query argument '%s' (default: %s)" % (query["name"], exp_value)}
                    elif exp_name == "in":
                        argument = {"name": '%s' % query["name"], "type": filter_cls(query.get("type_args")),
                                    "nargs": "+", "action": "extend", "default": exp_value,
                                    "help": "query arguments '%s' (default: %s)" % (query["name"], " ".join([str(ev) for ev in exp_value]))}
                    else:
                        argument = {"name": '%s__%s' % (query["name"], exp_name), "type": filter_cls(query.get("type_args")),
                                    "default": exp_value, "help": "query argument '%s__%s' (default: %s)" % (query["name"], exp_name, exp_value)}
                    if argument["name"] in arguments_names:
                        self.compile_merge_argument(arguments_names[argument["name"]], argument)
                        continue
                    arguments.append(argument)
                    arguments_names[argument["name"]] = argument

    def compile_sources_arguments(self, arguments_names, arguments, config):
        if isinstance(config, dict):
            for key, value in config.items():
                self.compile_sources_arguments(arguments_names, arguments, value)
        elif isinstance(config, list):
            for value in config:
                self.compile_sources_arguments(arguments_names, arguments, value)
        elif isinstance(config, str):
            if config[:1] != "?":
                return
            name = config[1:]
            keys = name.split("|")
            filters = (keys[1] if len(keys) >= 2 else "").split(" ")
            filter_cls = self.find_filter_driver(filters[0])
            filter_args = (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None
            argument = {"name": keys[0], "type": filter_cls(filter_args) if filter_cls else str, "help": "%s" % keys[0]}
            if argument["name"] in arguments_names:
                self.compile_merge_argument(arguments_names[argument["name"]], argument)
                return
            arguments.append(argument)
            arguments_names[argument["name"]] = argument

    def compile_arguments(self):
        arguments_names, arguments = {}, []
        for name, argument in self.config["arguments"].items():
            keys = name.split("|")
            filters = (keys[1] if len(keys) >= 2 else "").split(" ")
            filter_cls = self.find_filter_driver(filters[0])
            filter_args = (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None

            if isinstance(argument, dict):
                argument["name"] = keys[0]
                if "default" in argument:
                    argument["default"] = self.run_valuer(argument["default"], {})
                if "type" not in argument:
                    if filter_cls:
                        argument["type"] = filter_cls(filter_args)
                    else:
                        argument["type"] = type(argument.get("default", ""))
                if "help" not in argument:
                    argument["help"] = "%s (default: %s)" % (name, argument.get("default", ""))
            else:
                argument = self.run_valuer(argument, {})
                if filter_cls:
                    argument_type = filter_cls(filter_args)
                else:
                    argument_type = type(argument)
                argument = {"name":  keys[0], "type": argument_type, "default": argument,
                            "help": "%s (default: %s)" % (name, argument)}

            if argument["name"] in arguments_names:
                self.compile_merge_argument(arguments_names[argument["name"]], argument)
                continue
            arguments.append(argument)
            arguments_names[argument["name"]] = argument

        self.compile_sources_arguments(arguments_names, arguments, self.config)
        self.compile_filters_arguments(arguments_names, arguments)

        if "input" in self.config and "@input" not in arguments_names:
            if isinstance(self.config["input"], list) and self.config["input"] and self.config["input"][0][0] == "@":
                self.config["input"] = self.run_valuer(self.config["input"], {})
            if self.config["input"][:2] == "<<":
                arguments.append({"name": "@input", "short": "i", "type": str, "default": self.config["input"][2:],
                                  "help": "data input (default: %s)" % self.config["input"][2:]})

        if "loader" in self.config and "@loader" not in arguments_names:
            if isinstance(self.config["loader"], list) and self.config["loader"] and self.config["loader"][0][0] == "@":
                self.config["loader"] = self.run_valuer(self.config["loader"], {})
            if self.config["loader"][:2] == "<<":
                arguments.append({"name": "@loader", "type": str, "default": self.config["loader"][2:],
                                  "choices": ("db_loader",),
                                  "help": "data loader (default: %s)" % self.config["loader"][2:]})

        if "output" in self.config and "@output" not in arguments_names:
            if isinstance(self.config["output"], list) and self.config["output"] and self.config["output"][0][0] == "@":
                self.config["output"] = self.run_valuer(self.config["output"], {})
            if self.config["output"][:2] == ">>":
                arguments.append({"name": "@output", "short": "o", "type": str, "default": self.config["output"][2:],
                                  "help": "data output (default: %s)" % self.config["output"][2:]})

        if "outputer" in self.config and "@outputer" not in arguments_names:
            if isinstance(self.config["outputer"], list) and self.config["outputer"] and self.config["outputer"][0][0] == "@":
                self.config["outputer"] = self.run_valuer(self.config["outputer"], {})
            if self.config["outputer"][:2] == ">>":
                arguments.append({"name": "@outputer", "type": str, "default": self.config["outputer"][2:],
                                  "choices": tuple(self.outputer_creater.can_uses()),
                                  "help": "data outputer (default: %s)" % self.config["outputer"][2:]})

        if "@limit" not in arguments_names:
            arguments.append({"name": "@limit", "short": "l", "type": int, "default": 0,
                              "help": "load limit count (default: 0 all)"})
        if "@batch" not in arguments_names:
            arguments.append({"name": "@batch", "short": "b", "type": int, "default": 0,
                              "help": "per sync batch count (default: 0 all)"})
        if "@recovery" not in arguments_names:
            arguments.append({"name": "@recovery", "short": "r", "type": bool, "default": False,
                              "help": "recovery mode (default: False)"})
        if "@join_batch" not in arguments_names:
            arguments.append({"name": "@join_batch", "type": int, "default": 1000,
                              "help": "join batch count (default: 1000)"})
        if "@insert_batch" not in arguments_names:
            arguments.append({"name": "@insert_batch", "type": int, "default": 0,
                              "help": "insert batch count (default: 0 all)"})
        if "@timeout" not in arguments_names:
            arguments.append({"name": "@timeout", "type": int, "default": 0,
                              "help": "loader timeout (default: 0 none timeout)"})
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
        if isinstance(foreign_key, list):
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
                keys = key.split("|")
                filters = (keys[1] if len(keys) >= 2 else "").split(" ")
                filter_cls = self.find_filter_driver(filters[0])
                filter_args = (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None

                if isinstance(exps, dict):
                    for exp, value in exps.items():
                        try:
                            exp = get_expression_name(exp)
                            value = self.run_valuer(value, {})
                            if filter_cls:
                                value = filter_cls(filter_args).filter(value)
                            foreign_filters.append((keys[0], exp, value))
                        except KeyError:
                            pass
                else:
                    value = self.run_valuer(exps, {})
                    if filter_cls:
                        value = filter_cls(filter_args).filter(value)
                    foreign_filters.append((keys[0], 'eq', value))

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
            self.schema = {}
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
                if "#case" in valuer:
                    case_case = valuer.pop("#case")
                    cases = {}
                    case_default = valuer.pop("#end") if "#end" in valuer else None
                    case_return = valuer.pop("::") if "::" in valuer else None
                    for case_key, case_value in valuer.items():
                        if case_key and isinstance(case_key, str) and case_key[0] == ":" and case_key[1:].isdigit():
                            cases[int(case_key[1:])] = case_value
                        else:
                            cases[case_key] = case_value
                    return self.valuer_compiler.compile_case_valuer('', None, case_case, cases, case_default, case_return)

                if "#match" in valuer:
                    match_match = valuer.pop("#match")
                    matchs = {}
                    match_default = valuer.pop("#end") if "#end" in valuer else None
                    match_return = valuer.pop("::") if "::" in valuer else None
                    for match_key, match_value in valuer.items():
                        if not isinstance(match_key, str):
                            continue
                        matchs[match_key] = match_value
                    return self.valuer_compiler.compile_match_valuer('', None, match_match, matchs, match_default, match_return)
                return self.valuer_compiler.compile_const_valuer(valuer)
            return valuer

        if isinstance(valuer, list):
            if not valuer:
                return self.valuer_compiler.compile_const_valuer(valuer)

            key = self.compile_key(valuer[0])
            if key["instance"] is None:
                return self.valuer_compiler.compile_const_valuer(valuer)

            if key["instance"] == "$":
                if len(valuer) not in (2, 3):
                    if "inherit_reflen" in key and key["inherit_reflen"] > 0:
                        return self.valuer_compiler.compile_inherit_valuer(key["key"], key["filter"], key["inherit_reflen"])
                    return self.valuer_compiler.compile_data_valuer(key["key"], key["filter"])

                if len(valuer) == 2:
                    if isinstance(valuer[1], str) and valuer[1][:1] == ":":
                        return self.valuer_compiler.compile_data_valuer(key["key"], key["filter"], valuer[1])
                    if isinstance(valuer[1], list) and valuer[1][0][:1] == ":":
                        return self.valuer_compiler.compile_data_valuer(key["key"], key["filter"], valuer[1])

                foreign_key = self.compile_foreign_key(valuer[1])
                if foreign_key is None:
                    return self.valuer_compiler.compile_const_valuer(valuer)

                loader = {"name": "db_join_loader", "database": foreign_key["database"]}
                return self.valuer_compiler.compile_db_join_valuer(key["key"], loader, foreign_key["foreign_key"], foreign_key["foreign_filters"],
                                                   None, valuer[0], valuer[2] if len(valuer) >= 3 else None)

            if key["instance"] == "@":
                return self.valuer_compiler.compile_calculate_valuer(key["key"], key["filter"], valuer[1:])

            if key["instance"] == "#":
                if key["key"] == "const":
                    return self.valuer_compiler.compile_const_valuer(valuer[1:] if len(valuer) > 2 else
                                                                     (valuer[1] if len(valuer) > 1 else None))
                if key["key"] == "if" and len(valuer) in (3, 4, 5):
                    cases = {True: valuer[2]}
                    if len(valuer) == 4:
                        cases[False] = valuer[3]
                    return self.valuer_compiler.compile_if_valuer(key["key"], key["filter"], valuer[1],
                                                                  valuer[2], valuer[3] if len(valuer) >= 4 else None,
                                                                  valuer[4] if len(valuer) >= 5 else None)
                if key["key"] == "make" and len(valuer) in (2, 3):
                    return self.valuer_compiler.compile_make_valuer(key["key"], key["filter"], valuer[1],
                                                                    valuer[2] if len(valuer) >= 3 else None)
                if key["key"] == "let" and len(valuer) in (2, 3):
                    return self.valuer_compiler.compile_let_valuer(key["key"], key["filter"], valuer[1],
                                                                   valuer[2] if len(valuer) >= 3 else None)
                if key["key"] == "yield" and len(valuer) in (1, 2, 3):
                    return self.valuer_compiler.compile_yield_valuer(key["key"], key["filter"],
                                                                     valuer[1] if len(valuer) >= 1 else None,
                                                                     valuer[2] if len(valuer) >= 3 else None)
                if key["key"] == "aggregate" and len(valuer) in (2, 3):
                    return self.valuer_compiler.compile_aggregate_valuer(key["key"], key["filter"], valuer[1],
                                                                         valuer[2] if len(valuer) >= 3 else None)
                if key["key"] == "call" and len(valuer) in (2, 3, 4) and valuer[1] in self.config["defines"]:
                    return self.valuer_compiler.compile_call_valuer(valuer[1], key["filter"],
                                                                    valuer[2] if len(valuer) >= 3 else None,
                                                                    self.config["defines"][valuer[1]],
                                                                    valuer[3] if len(valuer) >= 4 else None)
                if key["key"] == "assign" and len(valuer) in (2, 3, 4):
                    return self.valuer_compiler.compile_assign_valuer(valuer[1], key["filter"],
                                                                      valuer[2] if len(valuer) >= 3 else None,
                                                                      valuer[3] if len(valuer) >= 4 else None)
                if key["key"] == "lambda" and len(valuer) == 2:
                    return self.valuer_compiler.compile_lambda_valuer(key["key"], key["filter"], valuer[1])
                if key["key"] == "foreach" and len(valuer) in (2, 3, 4):
                    return self.valuer_compiler.compile_foreach_valuer(key["key"], key["filter"], valuer[1],
                                                                      valuer[2] if len(valuer) >= 3 else None,
                                                                      valuer[3] if len(valuer) >= 4 else None)
                if key["key"] == "break" and len(valuer) == 2:
                    return self.valuer_compiler.compile_break_valuer(key["key"], key["filter"], valuer[1])
                if key["key"] == "continue" and len(valuer) == 2:
                    return self.valuer_compiler.compile_continue_valuer(key["key"], key["filter"], valuer[1])
                if key["key"] == "state" and len(valuer) in (2, 3, 4):
                    return self.valuer_compiler.compile_state_valuer(key["key"], key["filter"],
                                                                     valuer[1] if len(valuer) >= 2 else None,
                                                                     valuer[2] if len(valuer) >= 3 else None,
                                                                     valuer[3] if len(valuer) >= 4 else None)
                if key["key"] == "cache" and len(valuer) in (4, 5):
                    return self.valuer_compiler.compile_cache_valuer(valuer[1], key["filter"],
                                                                     valuer[2], valuer[3],
                                                                     valuer[4] if len(valuer) >= 5 else None)
            return self.valuer_compiler.compile_const_valuer(valuer)

        key = self.compile_key(valuer)
        if key["instance"] is None:
            return self.valuer_compiler.compile_const_valuer(key["value"])

        if key["instance"] == "$":
            if "inherit_reflen" in key and key["inherit_reflen"] > 0:
                return self.valuer_compiler.compile_inherit_valuer(key["key"], key["filter"], key["inherit_reflen"])
            return self.valuer_compiler.compile_data_valuer(key["key"], key["filter"])

        if key["instance"] == "@":
            return self.valuer_compiler.compile_calculate_valuer(key["key"], key["filter"], [])

        if key["instance"] == "#":
            if key["key"] == "const":
                return self.valuer_compiler.compile_const_valuer(None)
            if key["key"] == "yield":
                return self.valuer_compiler.compile_yield_valuer(key["key"], key["filter"], None, None)
            if key["key"] == "break":
                return self.valuer_compiler.compile_break_valuer(key["key"], key["filter"], None)
            if key["key"] == "continue":
                return self.valuer_compiler.compile_continue_valuer(key["key"], key["filter"], None)
        return self.valuer_compiler.compile_const_valuer(valuer)

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
                                        global_variables=self.global_variables, global_states=self.states)
            valuers[current_type].append(valuer)

        pipelines_hooker = PipelinesHooker(**valuers)
        self.add_hooker(pipelines_hooker)

    def run_valuer(self, config, data):
        config_valuer = self.compile_valuer(config)
        if not config_valuer or config_valuer.get("name") == "const_valuer":
            return config
        inherit_valuers, yield_valuers, aggregate_valuers = [], [], []
        valuer = self.create_valuer(config_valuer, schema_field_name="", inherit_valuers=inherit_valuers,
                                    join_loaders=self.join_loaders, yield_valuers=yield_valuers,
                                    aggregate_valuers=aggregate_valuers, define_valuers={},
                                    global_variables=dict(**self.config["variables"]), global_states=self.states)
        if not valuer:
            return config
        valuer.fill(data)
        value = valuer.get()
        if isinstance(value, types.GeneratorType):
            oyield = value
            while True:
                try:
                    value = oyield.send({"value": value, "data": data})
                except StopIteration:
                    return value
        return value

    def create_valuer(self, config, **kwargs):
        if "name" not in config or not config["name"]:
            raise ValuerUnknownException(config["name"] + " is unknown")

        if not hasattr(self.valuer_creater, "create_" + config["name"]):
            valuer_cls = self.find_valuer_driver(config["name"])
            if not valuer_cls:
                raise ValuerUnknownException(config["name"] + " is unknown")
            config = {key: value for key, value in config.items() if key != "name"}
            return valuer_cls(**config)

        return getattr(self.valuer_creater, "create_" + config["name"])(config, **kwargs)

    def create_loader(self, config, primary_keys):
        if "name" not in config or not config["name"]:
            raise LoaderUnknownException(config["name"] + " is unknown")

        if not hasattr(self.loader_creater, "create_" + config["name"]):
            loader_cls = self.find_loader_driver(config["name"])
            if not loader_cls:
                raise LoaderUnknownException(config["name"] + " is unknown")
            config = {key: value for key, value in config.items() if key != "name"}
            return loader_cls(**config)

        return getattr(self.loader_creater, "create_" + config["name"])(config, primary_keys)

    def create_outputer(self, config, primary_keys):
        if "name" not in config or not config["name"]:
            raise OutputerUnknownException(config["name"] + " is unknown")

        if not hasattr(self.outputer_creater, "create_" + config["name"]):
            outputer_cls = self.find_outputer_driver(config["name"])
            if not outputer_cls:
                raise OutputerUnknownException(config["name"] + " is unknown")
            config = {key: value for key, value in config.items() if key != "name"}
            return outputer_cls(**config)

        return getattr(self.outputer_creater, "create_" + config["name"])(config, primary_keys)

    def compile_loader(self):
        if self.config["input"][:2] == "<<":
            self.config["input"] = self.arguments.get("@input", self.config["input"][2:])
        if " use " in self.config["input"]:
            input_info = self.config["input"].split(" use ")
            if len(input_info) == 2:
                short_names = {"P": "pull"}
                if input_info[1] in short_names:
                    input_info[1] = short_names[input_info[1]]
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
            aggregate_valuers = []
            for name, valuer in self.schema.items():
                inherit_valuers, yield_valuers = [], []
                valuer = self.create_valuer(valuer, schema_field_name=name, inherit_valuers=inherit_valuers,
                                            join_loaders=self.join_loaders, yield_valuers=yield_valuers,
                                            aggregate_valuers=aggregate_valuers, define_valuers={},
                                            global_variables=self.global_variables, global_states=self.states)
                if valuer:
                    self.loader.add_valuer(name, valuer)
                if inherit_valuers:
                    raise OverflowError(name + " inherit out of range")
                if yield_valuers or aggregate_valuers:
                    loader_config["is_yield"] = True
                    self.loader.is_yield = True
        elif self.schema == ".*":
            self.loader.add_key_matcher(".*", self.create_valuer(self.valuer_compiler.compile_data_valuer("", None)))

        for query in self.config["querys"]:
            for exp_name in query["exps"]:
                if not hasattr(self.loader, "filter_%s" % exp_name):
                    continue
                if exp_name in query["refs"]:
                    argument_name = query["refs"][exp_name]
                elif exp_name in ("eq", "in"):
                    argument_name = query["name"]
                else:
                    argument_name = "%s__%s" % (query["name"], exp_name)
                if argument_name not in self.arguments:
                    continue
                getattr(self.loader, "filter_%s" % exp_name)(query["name"], self.arguments[argument_name])

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
                valuer = self.create_valuer(self.valuer_compiler.compile_data_valuer(name, None))
                if not valuer:
                    continue
                if name in self.loader.schema:
                    valuer.filter = self.loader.schema[name].get_final_filter()
                if name in self.config["options"]["schema"] and \
                        self.config["options"]["schema"][name].get("chaned_require_update"):
                    valuer.option = DataValuerOutputerOption(True)
                self.outputer.add_valuer(name, valuer)

        for query in self.config["querys"]:
            query_name = query["name"]
            if query_name not in self.outputer.schema:
                for field_name, valuer in self.schema.items():
                    if valuer.get("key") != query_name:
                        continue
                    if valuer.get("name") == "data_valuer" and not valuer.get("return_valuer"):
                        query_name = field_name
                        break

            if query_name not in self.outputer.schema:
                continue
            valuer = self.outputer.schema[query_name]
            if valuer.filter:
                value_filter = valuer.filter.filter
            else:
                value_filter = lambda v: v

            for exp_name in query["exps"]:
                if not hasattr(self.outputer, "filter_%s" % exp_name):
                    continue
                if exp_name in query["refs"]:
                    argument_name = query["refs"][exp_name]
                elif exp_name == "eq":
                    argument_name = query["name"]
                else:
                    argument_name = "%s__%s" % (query["name"], exp_name)
                if argument_name not in self.arguments:
                    continue
                getattr(self.outputer, "filter_%s" % exp_name)(query_name, value_filter(self.arguments[argument_name]))

    def merge_statistics(self, statistics, child_statistics):
        for key, value in child_statistics.items():
            if key not in statistics:
                statistics[key] = value
            elif isinstance(value, (int, float)):
                statistics[key] += value
            else:
                statistics[key] = value

    def print_queryed_statistics(self, loader, loader_statistics=None):
        statistics = loader.statistics()
        if isinstance(loader_statistics, dict):
            self.merge_statistics(loader_statistics, statistics)
            if "execute_time" not in loader_statistics:
                loader_statistics["execute_time"] = (time.time() - self.status.start_time) * 1000
        statistics = ["loader_%s: %s" % (key, value) for key, value in statistics.items()]
        get_logger().info("%s loader: %s <- %s %s", self.name, loader.__class__.__name__, self.input, " ".join(statistics))

    def print_loaded_statistics(self, join_loaders, join_loader_statistics=None):
        statistics = {}
        for join_loader in join_loaders:
            self.merge_statistics(statistics, join_loader.statistics())
        if isinstance(join_loader_statistics, dict):
            self.merge_statistics(join_loader_statistics, statistics)
            if "execute_time" not in join_loader_statistics:
                join_loader_statistics["execute_time"] = (time.time() - self.status.start_time) * 1000
        get_logger().info("%s join_count: %s %s", self.name, len(join_loaders),
                          " ".join(["join_%s: %s" % (key, value) for key, value in statistics.items()]))

    def print_stored_statistics(self, outputer, outputer_statistics=None):
        statistics = outputer.statistics()
        if isinstance(outputer_statistics, dict):
            self.merge_statistics(outputer_statistics, statistics)
            if "execute_time" not in outputer_statistics:
                outputer_statistics["execute_time"] = (time.time() - self.status.start_time) * 1000
        statistics = ["outputer_%s: %s" % (key, value) for key, value in statistics.items()]
        get_logger().info("%s outputer: %s -> %s %s", self.name, outputer.__class__.__name__, self.output, " ".join(statistics))

    def print_statistics(self, loader_name, loader_statistics, outputer_name, outputer_statistics,
                                 join_loader_count, join_loader_statistics):
        statistics = ["loader_%s: %s" % (key, value) for key, value in loader_statistics.items()]
        get_logger().info("%s loader: %s <- %s %s", self.name, loader_name, self.input, " ".join(statistics))

        get_logger().info("%s join_count: %s %s", self.name, join_loader_count,
                          " ".join(["join_%s: %s" % (key, value) for key, value in join_loader_statistics.items()]))

        statistics = ["outputer_%s: %s" % (key, value) for key, value in outputer_statistics.items()]
        get_logger().info("%s outputer: %s -> %s %s", self.name, outputer_name, self.output, " ".join(statistics))

    def get_dependencys(self):
        if "dependencys" not in self.config:
            return []

        if isinstance(self.config["dependencys"], list):
            return self.config["dependencys"]
        return [self.config["dependencys"]]

    def load(self):
        super(CoreTasker, self).load()
        self.load_config(self.config_filename)
        self.name = self.config["name"]
        self.load_imports()
        self.load_sources()
        self.load_states()
        return self.compile_arguments()

    def compile(self, arguments):
        super(CoreTasker, self).compile(arguments)

        self.compile_sources(self.config)
        self.compile_options()
        self.load_databases()
        self.load_caches()
        self.compile_variables()
        self.compile_schema()
        self.compile_pipelines()
        self.compile_loader()
        self.compile_outputer()
        self.input = self.config["input"]
        self.output = self.config["output"]
        for hooker in self.hookers:
            hooker.compiled(self)

    def run_batch(self, batch_count, loader_timeout):
        self.load_cursor()
        batch_index, load_count, last_cursor_data = 0, 0, self.batch_cursor
        get_logger().info("%s batch start %s, %s cursor: %s", self.name, 1, batch_count, "")

        while not self.terminated:
            batch_index += 1
            self.loader, self.outputer = self.loader.clone(), self.outputer.clone()
            self.join_loaders = {key: join_loader.clone() for key, join_loader in self.join_loaders.items()}

            if self.batch_cursor:
                self.loader.filter_cursor(self.batch_cursor, (batch_index - 1) * batch_count, batch_count)
                vcursor = ["%s -> %s" % (primary_key, self.batch_cursor.get(primary_key, ''))
                           for primary_key in self.loader.primary_keys]
                get_logger().info("%s batch start %s, %s cursor: %s", self.name,
                                  batch_index, batch_count, " ".join(vcursor))

            self.loader.filter_limit(batch_count)
            self.loader.load(loader_timeout)
            load_count = len(self.loader.datas)
            for hooker in self.hookers:
                self.loader.datas = hooker.queried(self, self.loader.datas)
            self.print_queryed_statistics(self.loader, self.status["statistics"]["loader"])

            datas = self.loader.get()
            if not datas:
                self.print_loaded_statistics(self.join_loaders.values(), self.status["statistics"]["join_loaders"])
                self.print_stored_statistics(self.outputer, self.status["statistics"]["outputer"])
                break
            for hooker in self.hookers:
                datas = hooker.loaded(self, datas)
            self.print_loaded_statistics(self.join_loaders.values(), self.status["statistics"]["join_loaders"])

            if last_cursor_data:
                self.outputer.filter_cursor(last_cursor_data, (batch_index - 1) * batch_count, batch_count)
            self.outputer.store(datas)
            for hooker in self.hookers:
                hooker.outputed(self, datas)
            self.print_stored_statistics(self.outputer, self.status["statistics"]["outputer"])
            self.batch_cursor, last_cursor_data = self.loader.last_data, datas[-1]
            self.status["data"]["first"] = datas[0]
            self.status["data"]["last"] = datas[-1]
            if load_count < batch_count:
                break
            for name, database in self.databases.items():
                database.flush()
            self.states.save(self)

        get_logger().info("%s batch end %s, %s", self.name, batch_index - 1, batch_count)
        statistics = (self.loader.__class__.__name__, self.status["statistics"]["loader"], self.outputer.__class__.__name__,
                      self.status["statistics"]["outputer"], len(self.join_loaders), self.status["statistics"]["join_loaders"])
        self.print_statistics(*statistics)
        return self.loader.next()

    def run_once(self, loader_timeout):
        if "@limit" in self.arguments and self.arguments["@limit"] > 0:
            self.loader.filter_limit(self.arguments["@limit"])
        self.loader.load(loader_timeout)
        for hooker in self.hookers:
            self.loader.datas = hooker.queried(self, self.loader.datas)
        self.print_queryed_statistics(self.loader, self.status["statistics"]["loader"])

        datas = self.loader.get()
        if not datas:
            self.print_loaded_statistics(self.join_loaders.values(), self.status["statistics"]["join_loaders"])
            self.print_stored_statistics(self.outputer, self.status["statistics"]["outputer"])
            return self.loader.next()
        for hooker in self.hookers:
            datas = hooker.loaded(self, datas)
        self.print_loaded_statistics(self.join_loaders.values(), self.status["statistics"]["join_loaders"])

        self.outputer.store(datas)
        for hooker in self.hookers:
            hooker.outputed(self, datas)
        self.print_stored_statistics(self.outputer, self.status["statistics"]["outputer"])
        self.status["data"]["first"] = datas[0]
        self.status["data"]["last"] = datas[-1]
        return self.loader.next()

    def run(self):
        get_logger().info("%s start %s -> %s", self.name, self.config_filename, self.config.get("name"))
        super(CoreTasker, self).run()
        batch_count = int(self.arguments.get("@batch", 0))
        loader_timeout = int(self.arguments.get("@timeout", None))

        while not self.terminated:
            if batch_count > 0:
                if not self.run_batch(batch_count, loader_timeout):
                    break
                continue

            if not self.run_once(loader_timeout):
                break
            self.loader = self.loader.clone()
            self.outputer = self.outputer.clone()
            self.join_loaders = {key: join_loader.clone() for key, join_loader in self.join_loaders.items()}

        self.status["execute_time"] = (time.time() - self.status.start_time) * 1000
        get_logger().info("%s finish %s %s %.2fms", self.name, self.config_filename, self.config.get("name"),
                          self.status["execute_time"])

    def terminate(self):
        if self.terminated:
            return
        self.terminated = True
        if hasattr(self.loader, "terminate") and callable(self.loader.terminate):
            self.loader.terminate()
        if hasattr(self.outputer, "terminate") and callable(self.outputer.terminate):
            self.outputer.terminate()

    def close(self, succed=True, message="", traceback=""):
        if self.closed:
            return
        self.closed = True
        self.status["status"] = "succed" if succed else "fail"
        self.status["message"] = message
        self.status["trackback"] = traceback
        self.states.save(self)
        for name, database in self.databases.items():
            database.close()
        self.states.close()
        self.valuer_compiler, self.valuer_creater, self.loader_creater, self.outputer_creater = None, None, None, None

    def get_status(self):
        if "runner_id" not in self.status or not self.status["runner_id"]:
            self.status["runner_id"] = gen_runner_id()
        status = {key: value for key, value in self.status.items()}
        status["name"] = self.name
        status["start_time"] = datetime.datetime.fromtimestamp(self.status.start_time, pytz.UTC)
        status["arguments"] = dict(**self.arguments)
        status["variables"] = dict(**self.global_variables)
        status["states"] = dict(**self.states)
        if self.loader and isinstance(self.batch_cursor, dict):
            if len(self.loader.primary_keys) > 1:
                status["cursor"] = self.batch_cursor
            else:
                status["cursor"] = self.batch_cursor.get(self.loader.primary_keys[0])
        else:
            status["cursor"] = self.batch_cursor
        return status