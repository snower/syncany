# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import time
import argparse
import datetime
import logging
import logging.config
import traceback
import json
from .tasker import Tasker
from ..database import find_database
from ..loaders import find_loader
from ..valuers import find_valuer
from ..filters import find_filter
from ..outputers import find_outputer
from ..utils import get_expression_name

class ValuerCompiler(object):
    def compile_const_valuer(self, value = None):
        return {
            "name": "const_valuer",
            "value": value
        }

    def compile_db_valuer(self, key = "", filter = None):
        return {
            "name": "db_valuer",
            "key": key,
            "filter": filter
        }

    def compile_const_join_valuer(self, key = "", value = None, loader = None, foreign_key = "", valuer = None):
        valuer = self.compile_schema_field(valuer)

        return {
            "name": "const_join_valuer",
            "key": key,
            "value": value,
            "loader": loader,
            "foreign_key": foreign_key,
            "valuer": valuer,
        }

    def compile_db_join_valuer(self, key = "", loader = None, foreign_key = "", filter = None, valuer = None):
        valuer = self.compile_schema_field(valuer)

        return {
            "name": "db_join_valuer",
            "key": key,
            "loader": loader,
            "foreign_key": foreign_key,
            "valuer": valuer,
            "filter": filter,
        }

    def compile_case_valuer(self, key = "", case = {}, default_case = None):
        case_valuers = {}
        if isinstance(case, list):
            for index in range(len(case)):
                case_valuers[index] = self.compile_schema_field(case[index])
        else:
            for key, field in case.items():
                case_valuers[key] = self.compile_schema_field(case[field])

        if default_case:
            default_case = self.compile_schema_field(default_case)

        return {
            "name": "case_valuer",
            "key": key,
            "case": case_valuers,
            "default_case": default_case,
        }

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
            loader_cache_key = config["loader"]["database"] + "::" + config["foreign_key"]
            if loader_cache_key in join_loaders:
                loader = join_loaders[loader_cache_key]
            else:
                loader = self.create_loader(config["loader"], [config["foreign_key"]])
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
        return valuer_cls(loader, config["foreign_key"], child_valuer, config["key"], filter)

    def create_case_valuer(self, config, join_loaders = None):
        valuer_cls = find_valuer(config["name"])
        if not valuer_cls:
            return
        case_valuers = {}
        for key, valuer_config in config["case"].items():
            case_valuers[key] = self.create_valuer(valuer_config, join_loaders)
        default_case_valuer = self.create_valuer(config["default_case"], join_loaders) \
            if "default_case" in config and config["default_case"] else None
        return valuer_cls(case_valuers, default_case_valuer, config["key"], None)

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

class OutputerCreater(object):
    def create_db_update_insert_outputer(self, config, primary_keys):
        outputer_cls = find_outputer(config["name"])
        if not outputer_cls:
            return None
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys)

    def create_db_delete_insert_outputer(self, config, primary_keys):
        outputer_cls = find_outputer(config["name"])
        if outputer_cls:
            return None
        db_name = config["database"].split(".")[0]
        return outputer_cls(self.databases[db_name], config["database"], primary_keys)

class JsonTasker(Tasker, ValuerCompiler, ValuerCreater, LoaderCreater, OutputerCreater):
    def __init__(self, json_filename):
        self.json_filename = json_filename
        super(JsonTasker, self).__init__()

        self.start_time = time.time()
        self.config = {}
        self.name = ""
        self.join_loaders = {}

        self.valuer_compiler = {
            "const_valuer": self.compile_const_valuer,
            "db_valuer": self.compile_db_valuer,
            "const_join_valuer": self.compile_const_join_valuer,
            "db_join_valuer": self.compile_db_join_valuer,
            "case_valuer": self.compile_case_valuer,
        }

        self.valuer_creater = {
            "const_valuer": self.create_const_valuer,
            "db_valuer": self.create_db_valuer,
            "const_join_valuer": self.create_const_join_valuer,
            "db_join_valuer": self.create_db_join_valuer,
            "case_valuer": self.create_case_valuer,
        }

        self.loader_creater = {
            "const_loader": self.create_const_loader,
            "db_loader": self.create_db_loader,
            "db_join_loader": self.create_db_join_loader,
        }

        self.outputer_creater = {
            "db_update_insert_outputer": self.create_db_update_insert_outputer,
            "db_delete_insert_outputer": self.create_db_delete_insert_outputer,
        }

    def load_json(self, filename):
        with open(filename, "r") as fp:
            config = json.load(fp)
            if "extends" in config:
                if isinstance(config["extends"], list):
                    for json_filename in config["extends"]:
                        self.load_json(json_filename)
                else:
                    self.load_json(config["extends"])
            self.config.update(config)

        self.name = self.config["name"]
        self.input = self.config["input"]
        self.output = self.config["output"]

    def load_databases(self):
        for config in self.config["databases"]:
            database_cls = find_database(config.pop("driver"))
            self.databases[config["name"]] = database_cls(config)

    def compile_logging(self):
        if "logging" in self.config and isinstance(self.config["logging"], dict):
            logging.config.dictConfig(self.config["logging"])

    def compile_filters(self):
        TYPES = {
            "int": int,
            "str": str,
            "float": float,
            "datetime": lambda v: datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S"),
            "date": lambda v: datetime.datetime.strptime(v, "%Y-%m-%d"),
        }

        self.argparse.add_argument("json", type=str, nargs=argparse.OPTIONAL, help="json filename")
        for filter in self.config["filters"]:
            if "exps" in filter:
                if isinstance(filter["exps"], str):
                    filter["exps"] = [filter["exps"]]

                if isinstance(filter["exps"], list):
                    for exp in filter["exps"]:
                        exp_name = get_expression_name(exp)
                        self.argparse.add_argument('--%s__%s' % (filter["name"], exp_name), dest="%s_%s" % (filter["name"], exp_name),
                                                   type=TYPES[filter["type"]], required=True, help="%s %s" % (filter["name"], exp))
                elif isinstance(filter["exps"], dict):
                    for exp, value in filter["exps"].items():
                        exp_name = get_expression_name(exp)
                        self.argparse.add_argument('--%s__%s' % (filter["name"], exp_name), dest="%s_%s" % (filter["name"], exp_name),
                                               type=TYPES[filter["type"]], default=value, help="%s %s" % (filter["name"], exp))
            else:
                self.argparse.add_argument('--%s' % filter["name"], dest="%s" % filter["name"], type=TYPES[filter["type"]], help="%s" % filter["name"])

    def compile_key(self, key):
        if not isinstance(key, str) or key == "":
            return {"instance": None, "key": "", "value": key, "filter": None}

        if key[0] == "$":
            instance, key = "$", key[2:]
        elif key[0] == "&":
            instance, key = "&", key[2:]
        else:
            return {"instance": None, "key": "", "value": key, "filter": None}

        key_filters = key.split("|")
        key = key_filters[0]

        filter, filter_args = None, None
        if len(key_filters) >= 2:
            filters, filter = key_filters[1].split(" ")
            filter = filters[0]
            if len(filters) >= 2:
                filter_args = "".join(filters[1]) + "".join(key_filters[2:])

        return {
            "instance": instance,
            "key": key,
            'value': None,
            "filter": {
                "name": filter,
                "args": filter_args
            },
        }

    def compile_foreign_key(self, foreign_key):
        if foreign_key[0] == "&":
            foreign_key = foreign_key[2:]
        foreign_key = foreign_key.split("::")

        return {
            "database": foreign_key[0],
            "foreign_key": foreign_key[1],
        }

    def compile_schema(self):
        for name, field in self.config["schema"].items():
            self.schema[name] = self.compile_schema_field(field)

    def compile_schema_field(self, field):
        if isinstance(field, dict):
            name = "compile_" + field.pop("name")
            return getattr(self, name)(**field)

        if isinstance(field, list):
            key = self.compile_key(field[0])
            if key["instance"] is None:
                foreign_key = self.compile_foreign_key(field[1])
                loader = {"name": "db_join_loader", "database": foreign_key["database"]}
                return self.compile_const_join_valuer(key["key"], key["value"], loader, foreign_key["foreign_key"], field[2])

            if key["instance"] == "$":
                foreign_key = self.compile_foreign_key(field[1])
                loader = {"name": "db_join_loader", "database": foreign_key["database"]}
                return self.compile_db_join_valuer(key["key"], loader, foreign_key["foreign_key"], key["filter"], field[2])
            return None

        key = self.compile_key(field)
        if key["instance"] is None:
            return self.compile_const_valuer(key["value"])

        if key["instance"] == "$":
            return self.compile_db_valuer(key["key"], key["filter"])
        return None

    def create_valuer(self, config, join_loaders = None):
        if "name" not in config or config["name"]:
            return None

        if config["name"] not in self.valuer_creater:
            return None

        return self.valuer_creater[config["name"]](config, join_loaders)

    def create_loader(self, config, primary_keys):
        if "name" not in config or config["name"]:
            return None

        if config["name"] not in self.loader_creater:
            return None

        return self.loader_creater[config["name"]](config, primary_keys)

    def create_outputer(self, config, primary_keys):
        if "name" not in config or config["name"]:
            return None

        if config["name"] not in self.outputer_creater:
            return None

        return self.outputer_creater[config["name"]](config, primary_keys)

    def compile_loader(self):
        input_loader = self.compile_foreign_key(self.config["input"])
        db_name = input_loader["database"].split(".")[0]

        loader = self.config.get("loader", self.databases[db_name].get_default_loader())
        loader_config = {
            "name": loader,
            "database": input_loader["database"],
        }
        self.loader = self.create_loader(loader_config, [input_loader["foreign_key"]])

        for name, valuer in self.schema.items():
            valuer = self.create_valuer(valuer, self.join_loaders)
            if valuer:
                self.loader.add_valuer(name, valuer)

        for filter in self.config["filters"]:
            if "exps" in filter:
                if isinstance(filter["exps"], str):
                    exps = [filter["exps"]]
                else:
                    exps = filter["exps"]

                for exp in exps:
                    exp_name = get_expression_name(exp)
                    if hasattr(self.loader, "filter_%s" % exp_name) and hasattr(self.arguments, "%s_%s" % (filter["name"], exp_name)):
                        getattr(self.loader, "filter_%s" % exp_name)(filter["name"], getattr(self.arguments, "%s_%s" % (
                        filter["name"], exp_name)))
            else:
                if hasattr(self.loader, "filter_eq") and hasattr(self.arguments, filter["name"]):
                    getattr(self.loader, "filter_eq")(filter["name"], getattr(self.arguments, filter["name"]))

    def compile_outputer(self):
        output_outputer = self.compile_foreign_key(self.config["output"])
        db_name = output_outputer["database"].split(".")[0]

        outputer = self.config.get("outputer", self.databases[db_name].get_default_outputer())
        outputer_config = {
            "name": outputer,
            "database": output_outputer["database"],
        }
        self.outputer = self.create_outputer(outputer_config, [output_outputer["foreign_key"]])

        for name, valuer in self.schema.items():
            valuer = self.create_valuer(self.compile_db_valuer(name, None))
            if valuer:
                self.outputer.add_valuer(name, valuer)

        for filter in self.config["filters"]:
            if "exps" in filter:
                if isinstance(filter["exps"], str):
                    exps = [filter["exps"]]
                else:
                    exps = filter["exps"]

                for exp in exps:
                    exp_name = get_expression_name(exp)
                    if hasattr(self.outputer, "filter_%s" % exp_name) and hasattr(self.arguments, "%s_%s" % (filter["name"], exp_name)):
                        getattr(self.outputer, "filter_%s" % exp_name)(filter["name"], getattr(self.arguments, "%s_%s" % (filter["name"], exp_name)))
            else:
                if hasattr(self.outputer, "filter_eq") and hasattr(self.arguments, filter["name"]):
                    getattr(self.outputer, "filter_eq")(filter["name"], getattr(self.arguments, filter["name"]))

    def print_statistics(self):
        statistics = ["loader_%s: %s" % (key, value) for key, value in self.loader.statistics().items()]
        logging.info("loader: %s <- %s %s", self.loader.__class__.__name__, self.input, " ".join(statistics))

        statistics = {}
        for name, join_loader in self.join_loaders.items():
            for key, value in join_loader.statistics().items():
                if key not in statistics:
                    statistics[key] = value
                else:
                    statistics[key] += value

        logging.info("join_count: %s %s", len(self.join_loaders),
                     " ".join(["join_%s: %s" % (key, value) for key, value in statistics.items()]))

        statistics = ["outputer_%s: %s" % (key, value) for key, value in self.outputer.statistics().items()]
        logging.info("outputer: %s -> %s %s", self.outputer.__class__.__name__, self.output, " ".join(statistics))
        logging.info("finish %s %s %.2fms", self.json_filename, self.config.get("name"), (time.time() - self.start_time) * 1000)

    def run(self):
        try:
            self.load_json(self.json_filename)
            self.compile_logging()
            self.compile_filters()
            super(JsonTasker, self).run()

            self.load_databases()
            self.compile_schema()
            self.compile_loader()
            self.compile_outputer()

            datas = self.loader.get()
            if datas:
                self.outputer.store(datas)
            for name, database in self.databases.items():
                database.close()

            self.print_statistics()
        except Exception as e:
            logging.error("%s\n%s", e, traceback.format_exc())