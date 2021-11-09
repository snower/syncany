# -*- coding: utf-8 -*-
# 2021/11/6
# create by: snower

from ...logger import get_logger

class States(dict):
    def __init__(self):
        super(States, self).__init__()
        self.taskers = []

    def add_tasker(self, state_tasker):
        self.taskers.append(state_tasker)

    def compile_state(self, tasker, state_tasker):
        state_tasker.load_config(state_tasker.config_filename)
        if "name" not in state_tasker.config or not state_tasker.config["name"]:
            state_tasker.name = tasker.config["name"] + "::state"
        else:
            state_tasker.name = tasker.config["name"] + "::" + state_tasker.config["name"] + "::state"
        if "imports" not in state_tasker.config or not state_tasker.config["imports"]:
            state_tasker.config["databases"] = dict(**state_tasker.config["imports"])
        state_tasker.load_imports()
        state_tasker.load_sources()
        if "databases" not in state_tasker.config or not state_tasker.config["databases"]:
            state_tasker.config["databases"] = [database for database in tasker.config["databases"]]
        if "sources" not in state_tasker.config or not state_tasker.config["sources"]:
            state_tasker.config["sources"] = dict(**tasker.config["sources"])
        state_tasker.arguments["name"] = tasker.name
        state_tasker.compile_sources(state_tasker.config)
        state_tasker.compile_options()
        state_tasker.load_databases()
        state_tasker.load_caches()
        state_tasker.compile_schema()
        if "input" in state_tasker.config and state_tasker.config["input"]:
            state_tasker.compile_loader()
            for primary_key in state_tasker.loader.primary_keys:
                state_tasker.loader.order_by(primary_key, -1)
            state_tasker.loader.filter_limit(1)
            state_tasker.input = state_tasker.config["input"]
        else:
            state_tasker.config["input"] = state_tasker.config["output"]
            state_tasker.compile_loader()
        if "output" in state_tasker.config and state_tasker.config["output"]:
            if "use" not in state_tasker.config["output"]:
                state_tasker.config["output"] = state_tasker.config["output"] + " use UI"
            state_tasker.compile_outputer()
            state_tasker.output = state_tasker.config["output"]
        else:
            state_tasker.config["output"] = state_tasker.config["input"]
            state_tasker.compile_outputer()

    def compile(self, tasker):
        for state_tasker in self.taskers:
            self.compile_state(tasker, state_tasker)

    def load_state(self, tasker, state_tasker):
        state_tasker.loader.load()
        state_tasker.print_queryed_statistics(state_tasker.loader)
        datas = state_tasker.loader.get()
        if state_tasker.join_loaders:
            state_tasker.print_loaded_statistics(state_tasker.join_loaders.values())
        if "name" in state_tasker.config and state_tasker.config["name"]:
            state_name = state_tasker.config["name"]
            self[state_name] = {}
        else:
            state_name = ""
        if not datas:
            return
        for key, value in datas[0].items():
            if state_name:
                self[state_name][key] = value
            else:
                self[key] = value

    def load(self, tasker):
        for state_tasker in self.taskers:
            if not state_tasker.input:
                continue
            self.load_state(tasker, state_tasker)

    def save_state(self, tasker, state_tasker, datas):
        state_tasker.loader.datas = datas
        state_tasker.loader.loaded = True
        datas = state_tasker.loader.get()
        if state_tasker.join_loaders:
            state_tasker.print_loaded_statistics(state_tasker.join_loaders.values())
        if not datas:
            return
        state_tasker.outputer.store(datas)
        state_tasker.print_stored_statistics(state_tasker.outputer)
        for name, database in state_tasker.databases.items():
            database.flush()

    def save(self, tasker):
        datas = [tasker.get_status()]
        for state_tasker in self.taskers:
            if not state_tasker.output:
                continue
            try:
                self.save_state(tasker, state_tasker, datas)
            except Exception as e:
                get_logger().error("state save Error: %s", e)

    def close(self):
        for state_tasker in self.taskers:
            try:
                state_tasker.close()
            except Exception as e:
                get_logger().error("state save Error: %s", e)