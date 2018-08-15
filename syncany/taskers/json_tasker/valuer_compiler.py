# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

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
                case_valuers[key] = self.compile_schema_field(field)

        if default_case:
            default_case = self.compile_schema_field(default_case)

        return {
            "name": "case_valuer",
            "key": key,
            "case": case_valuers,
            "default_case": default_case,
        }

    def compile_calculate_valuer(self, key="", args=[], filter = None):
        args_valuers = []
        if isinstance(args, list):
            for arg in args:
                args_valuers.append(self.compile_schema_field(arg))
        else:
            args_valuers.append(args)

        return {
            "name": "calculate_valuer",
            "key": key,
            "args": args_valuers,
        }

    def compile_schema_valuer(self, schema={}):
        schema_valuers = {}
        for key, field in schema.items():
            schema_valuers[key] = self.compile_schema_field(field)

        return {
            "name": "schema_valuer",
            "key": "",
            "schema": schema_valuers,
        }