# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import time
import argparse
import logging
import logging.config
import traceback
import json
from collections import OrderedDict
from ..tasker import Tasker
from ...filters import find_filter
from ...database import find_database
from ...loaders import find_loader
from ...valuers import find_valuer
from ...outputers import find_outputer
from ...utils import get_expression_name
from .valuer_compiler import ValuerCompiler
from .valuer_creater import ValuerCreater
from .loader_creater import LoaderCreater
from .outputer_creater import OutputerCreater

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
            "calculate_valuer": self.compile_calculate_valuer,
            "schema_valuer": self.compile_schema_valuer,
        }

        self.valuer_creater = {
            "const_valuer": self.create_const_valuer,
            "db_valuer": self.create_db_valuer,
            "const_join_valuer": self.create_const_join_valuer,
            "db_join_valuer": self.create_db_join_valuer,
            "case_valuer": self.create_case_valuer,
            "calculate_valuer": self.create_calculate_valuer,
            "schema_valuer": self.create_schema_valuer,
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

    def load_databases(self):
        for config in self.config["databases"]:
            database_cls = find_database(config.pop("driver"))
            self.databases[config["name"]] = database_cls(config)

    def compile_logging(self):
        if "logging" in self.config and isinstance(self.config["logging"], dict):
            logging.config.dictConfig(self.config["logging"])

    def compile_filters(self):
        if isinstance(self.config["querys"], str):
            keys = self.config["querys"].split("|")
            filters = (keys[1] if len(keys) >= 2 else "str").split(" ")
            self.config["querys"] = [{
                "name": keys[0],
                "type": filters[0],
                'type_args': (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None
            }]
        elif isinstance(self.config["querys"], dict):
            querys = []
            for name, exps in self.config["querys"].items():
                keys = name.split("|")
                filters = (keys[1] if len(keys) >= 2 else "str").split(" ")
                querys.append({
                    "name": keys[0],
                    "exps": exps,
                    "type": filters[0],
                    'type_args': (" ".join(filters[1:]) + "|".join(keys[2:])) if len(filters) >= 2 else None
                })
            self.config["querys"] = querys

        self.argparse.add_argument("json", type=str, nargs=argparse.OPTIONAL, help="json filename")
        for filter in self.config["querys"]:
            if "exps" in filter:
                if isinstance(filter["exps"], str):
                    filter["exps"] = [filter["exps"]]

                if isinstance(filter["exps"], list):
                    for exp in filter["exps"]:
                        exp_name = get_expression_name(exp)
                        filter_cls = find_filter(filter["type"])
                        if filter_cls is None:
                            filter_cls = find_filter('str')
                        self.argparse.add_argument('--%s__%s' % (filter["name"], exp_name), dest="%s_%s" % (filter["name"], exp_name),
                                                   type=filter_cls(filter.get("type_args")), required=True, help="%s %s" % (filter["name"], exp))
                elif isinstance(filter["exps"], dict):
                    for exp, value in filter["exps"].items():
                        exp_name = get_expression_name(exp)
                        filter_cls = find_filter(filter["type"])
                        if filter_cls is None:
                            filter_cls = find_filter('str')
                        self.argparse.add_argument('--%s__%s' % (filter["name"], exp_name), dest="%s_%s" % (filter["name"], exp_name),
                                               type=filter_cls(filter.get("type_args")), default=value, help="%s %s" % (filter["name"], exp))
            else:
                filter_cls = find_filter(filter["type"])
                if filter_cls is None:
                    filter_cls = find_filter('str')
                self.argparse.add_argument('--%s' % filter["name"], dest="%s" % filter["name"], type=filter_cls(filter.get("type_args")), help="%s" % filter["name"])

        self.argparse.add_argument('--__batch', dest="config_batch_count", type=int, default=0, help="per sync batch count (default: 0 all)")

    def compile_key(self, key):
        if not isinstance(key, str) or key == "":
            return {"instance": None, "key": "", "value": key, "filter": None}

        if key[0] not in ("&", "$", "@", "|"):
            return {"instance": None, "key": "", "value": key, "filter": None}

        if key[0] in ("&", "$"):
            tokens = key.split(".")
            instance = tokens[0]
            key = ".".join(tokens[1:])
            if key == "" and tokens[0] == "$":
                key = "*"
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
        elif foreign_key[0] != "&":
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
                        self.schema[name] = self.compile_schema_field(field)
                        continue

                    order_names[index] = name
                    schema[name] = self.compile_schema_field(field)
                else:
                    self.schema[name] = self.compile_schema_field(field)

            for name in order_names:
                if name:
                    self.schema[name] = schema[name]

    def compile_schema_field(self, field):
        if isinstance(field, dict):
            if "name" not in field or not field["name"].endswith("_valuer"):
                field = {
                    "name": "case_valuer",
                    "key": field.pop("case") if "case" in field else "",
                    "case": field,
                    "default_case": field.pop("end") if "" in field else None,
                }
                return self.compile_case_valuer(**field)

            name = "compile_" + field.pop("name")
            return getattr(self, name)(**field)

        if isinstance(field, list):
            key = self.compile_key(field[0])
            if key["instance"] is None:
                foreign_key = self.compile_foreign_key(field[1])
                if foreign_key is None:
                    return self.compile_const_valuer(key["value"])

                loader = {"name": "db_join_loader", "database": foreign_key["database"]}
                return self.compile_const_join_valuer(key["key"], key["value"], loader, foreign_key["foreign_key"], field[2])

            if key["instance"] == "$":
                foreign_key = self.compile_foreign_key(field[1])
                if foreign_key is None:
                    return self.compile_const_valuer(key["value"])

                loader = {"name": "db_join_loader", "database": foreign_key["database"]}
                return self.compile_db_join_valuer(key["key"], loader, foreign_key["foreign_key"], foreign_key["foreign_filters"], key["filter"], field[2])

            if key["instance"] == "@":
                return self.compile_calculate_valuer(key["key"], field[1:], key["filter"])
            return self.compile_const_valuer(field)

        key = self.compile_key(field)
        if key["instance"] is None:
            return self.compile_const_valuer(key["value"])

        if key["instance"] == "$":
            return self.compile_db_valuer(key["key"], key["filter"])

        if key["instance"] == "@":
            return self.compile_calculate_valuer(key["key"], [], key["filter"])
        return self.compile_const_valuer(field)

    def create_valuer(self, config, join_loaders = None):
        if "name" not in config or not config["name"]:
            return None

        if config["name"] not in self.valuer_creater:
            valuer_cls = find_valuer(config["name"])
            if not valuer_cls:
                return
            config = {key: value for key, value in config.items() if key != "name"}
            return valuer_cls(**config)

        return self.valuer_creater[config["name"]](config, join_loaders)

    def create_loader(self, config, primary_keys):
        if "name" not in config or not config["name"]:
            return None

        if config["name"] not in self.loader_creater:
            loader_cls = find_loader(config["name"])
            if not loader_cls:
                return None
            config = {key: value for key, value in config.items() if key != "name"}
            return loader_cls(**config)

        return self.loader_creater[config["name"]](config, primary_keys)

    def create_outputer(self, config, primary_keys):
        if "name" not in config or not config["name"]:
            return None

        if config["name"] not in self.outputer_creater:
            outputer_cls = find_outputer(config["name"])
            if not outputer_cls:
                return None
            config = {key: value for key, value in config.items() if key != "name"}
            return outputer_cls(**config)

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

        if isinstance(self.schema, dict):
            for name, valuer in self.schema.items():
                valuer = self.create_valuer(valuer, self.join_loaders)
                if valuer:
                    self.loader.add_valuer(name, valuer)
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
                    if hasattr(self.loader, "filter_%s" % exp_name) and hasattr(self.arguments, "%s_%s" % (filter_name, exp_name)):
                        getattr(self.loader, "filter_%s" % exp_name)(filter_name, getattr(self.arguments, "%s_%s" % (filter_name, exp_name)))
            else:
                if hasattr(self.loader, "filter_eq") and hasattr(self.arguments, filter_name):
                    getattr(self.loader, "filter_eq")(filter_name, getattr(self.arguments, filter_name))

    def compile_outputer(self):
        output_outputer = self.compile_foreign_key(self.config["output"])
        db_name = output_outputer["database"].split(".")[0]

        outputer = self.config.get("outputer", self.databases[db_name].get_default_outputer())
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
            value_filter = lambda v : v
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
                    if hasattr(self.outputer, "filter_%s" % exp_name) and hasattr(self.arguments, "%s_%s" % (filter_name, exp_name)):
                        value = value_filter(getattr(self.arguments, "%s_%s" % (filter_name, exp_name)))
                        getattr(self.outputer, "filter_%s" % exp_name)(filter_name, value)
            else:
                if hasattr(self.outputer, "filter_eq") and hasattr(self.arguments, filter_name):
                    value = value_filter(getattr(self.arguments, filter_name))
                    getattr(self.outputer, "filter_eq")(filter_name, value)

    def print_statistics(self, loader_name, loader_statistics, outputer_name, outputer_statistics, join_loader_count, join_loader_statistics):
        statistics = ["loader_%s: %s" % (key, value) for key, value in loader_statistics.items()]
        logging.info("loader: %s <- %s %s", loader_name, self.input, " ".join(statistics))

        logging.info("join_count: %s %s", join_loader_count, " ".join(["join_%s: %s" % (key, value) for key, value in join_loader_statistics.items()]))

        statistics = ["outputer_%s: %s" % (key, value) for key, value in outputer_statistics.items()]
        logging.info("outputer: %s -> %s %s", outputer_name, self.output, " ".join(statistics))

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

    def run(self):
        try:
            self.load_json(self.json_filename)
            self.name = self.config["name"]
            self.input = self.config["input"]
            self.output = self.config["output"]

            self.compile_logging()
            self.compile_filters()
            super(JsonTasker, self).run()

            self.load_databases()
            self.compile_schema()
            self.compile_loader()
            self.compile_outputer()

            config_batch_count = int(getattr(self.arguments, "config_batch_count", 0))
            if config_batch_count > 0:
                batch_index = 0
                loader_statistics = {}
                outputer_statistics = {}
                join_loaders_statistics = {}

                cursor_data = None
                logging.info("start %s -> %s batch cursor: %s", 1, config_batch_count, "")

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

                        logging.info("start %s -> %s batch cursor: %s", batch_index, config_batch_count, " ".join(vcursor))

                    loader.filter_limit(config_batch_count)
                    datas = loader.get()
                    if not datas:
                        break

                    cursor_data = datas[-1]
                    for primary_key in outputer.primary_keys:
                        outputer.filter_lte(primary_key, cursor_data.get(primary_key, ''))
                    outputer.store(datas)
                    self.print_statistics(*self.merge_statistics({}, {}, {}, loader, outputer, self.join_loaders.values()))
                    self.merge_statistics(loader_statistics, outputer_statistics, join_loaders_statistics, loader,
                                          outputer, self.join_loaders.values())

                logging.info("end %s -> %s batch show statistics", batch_index - 1, config_batch_count)
                self.print_statistics(self.loader.__class__.__name__, loader_statistics,
                                      self.outputer.__class__.__name__, outputer_statistics,
                                      len(self.join_loaders), join_loaders_statistics)

            else:
                datas = self.loader.get()
                if datas:
                    self.outputer.store(datas)
                self.print_statistics(*self.merge_statistics({}, {}, {}, self.loader, self.outputer, self.join_loaders.values()))

            for name, database in self.databases.items():
                database.close()

            logging.info("finish %s %s %.2fms", self.json_filename, self.config.get("name"), (time.time() - self.start_time) * 1000)
        except Exception as e:
            logging.error("%s\n%s", e, traceback.format_exc())