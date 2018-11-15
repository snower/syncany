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

    def compile_db_join_valuer(self, key = "", loader = None, foreign_key = "", foreign_filters = None, filter = None, valuer = None):
        valuer = self.compile_schema_field(valuer)

        return {
            "name": "db_join_valuer",
            "key": key,
            "loader": loader,
            "foreign_key": foreign_key,
            'foreign_filters': foreign_filters or [],
            "valuer": valuer,
            "filter": filter,
        }

    def compile_case_valuer(self, key = "", value = None, case = None, default_case = None):
        if value is not None:
            key, value = '', self.compile_schema_field(value)
        elif (key and key[0] in ("$", "@")) or isinstance(key, list):
            key, value = '', self.compile_schema_field(key)

        case_valuers = {}
        if isinstance(case, list):
            for index in range(len(case)):
                case_valuers[index] = self.compile_schema_field(case[index])
        elif isinstance(case, dict):
            for case_value, field in case.items():
                case_valuers[case_value] = self.compile_schema_field(field)

        if default_case:
            default_case = self.compile_schema_field(default_case)

        return {
            "name": "case_valuer",
            "key": key,
            'value': value,
            "case": case_valuers,
            "default_case": default_case,
        }

    def compile_calculate_valuer(self, key="", args=None, filter = None):
        args_valuers, return_valuer = [], None
        if isinstance(args, list):
            for arg in args:
                if arg and arg[0] == ":":
                    return_valuer = self.compile_schema_field(arg[1:])
                elif arg and isinstance(arg, (list, tuple, set)) and arg[0] and arg[0][0] == ":":
                    arg = list(arg)
                    arg[0] = arg[0][1:]
                    return_valuer = self.compile_schema_field(arg)
                else:
                    args_valuers.append(self.compile_schema_field(arg))
        else:
            args_valuers.append(args)

        return {
            "name": "calculate_valuer",
            "key": key,
            "args": args_valuers,
            "return": return_valuer,
            "filter": filter,
        }

    def compile_schema_valuer(self, schema=None):
        schema_valuers = {}

        if isinstance(schema, dict):
            for key, field in schema.items():
                schema_valuers[key] = self.compile_schema_field(field)

        return {
            "name": "schema_valuer",
            "key": "",
            "schema": schema_valuers,
        }