# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import datetime
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None

class ValuerCompiler(object):
    def __init__(self, tasker):
        self.tasker = tasker

    def parse_return_valuer(self, arg):
        if arg is None:
            return None, None

        if isinstance(arg, str):
            if arg[:1] == ":":
                return arg[1:], None
            return None, arg

        if isinstance(arg, list):
            if not arg or not isinstance(arg[0], str):
                return None, arg

            if arg[0] == ":":
                if len(arg) == 2 and isinstance(arg[1], dict):
                    return arg[1], None
                return arg[1:], None
            if arg[0][:1] == ":":
                arg[0] = arg[0][1:]
                return arg, None

        if isinstance(arg, dict):
            if ":#case" in arg:
                arg["#case"] = arg.pop(":#case")
                return arg, None
            if ":#match" in arg:
                arg["#match"] = arg.pop(":#match")
                return arg, None
            if "name" not in arg or not arg["name"].endswith("_valuer"):
                return None, arg
            return arg, None
        return None, arg

    def compile_valuer(self, *args, **kwargs):
        return self.tasker.compile_valuer(*args, **kwargs)

    def compile_const_valuer(self, value=None):
        filter = None
        filter_name = type(value).__name__
        if filter_name in ("int", "float", "str", 'bool'):
            filter = {"name": filter_name, "args": None}
        elif ObjectId is not None and isinstance(value, ObjectId):
            filter = {"name": "ObjectId", "args": None}
        elif isinstance(value, datetime.datetime):
            filter = {"name": "datetime", "args": None}
        elif isinstance(value, datetime.date):
            filter = {"name": "date", "args": None}

        return {
            "name": "const_valuer",
            "value": value,
            "filter": filter,
        }

    def compile_data_valuer(self, key="", filter=None, return_arg=None):
        data_return_arg, _ = self.parse_return_valuer(return_arg)
        if data_return_arg is not None:
            return_arg = data_return_arg

        return_valuer = self.compile_valuer(return_arg) if return_arg else None
        return {
            "name": "data_valuer",
            "key": key,
            "filter": filter,
            "return_valuer": return_valuer,
        }

    def compile_inherit_valuer(self, key="", filter=None, reflen=0):
        return {
            "name": "inherit_valuer",
            "key": key,
            "filter": None,
            'reflen': reflen,
            "value_valuer": self.compile_data_valuer(key, filter)
        }

    def compile_db_join_valuer(self, key="", loader=None, foreign_key="", foreign_filters=None, filter=None, args_arg=None, return_arg=None):
        join_return_arg, _ = self.parse_return_valuer(return_arg)
        if join_return_arg is not None:
            return_arg = join_return_arg
        else:
            return_arg = "$.*" if return_arg is None else return_arg

        args_valuer = self.compile_valuer(args_arg) if args_arg else None
        return_valuer = self.compile_valuer(return_arg)

        return {
            "name": "db_join_valuer",
            "key": key,
            "filter": filter,
            "loader": loader,
            "foreign_key": foreign_key,
            'foreign_filters': foreign_filters or [],
            "args_valuer": args_valuer,
            "return_valuer": return_valuer,
        }

    def compile_case_valuer(self, key="", filter=None, value_arg=None, cases_arg=None, default_arg=None, return_arg=None):
        case_return_arg, _ = self.parse_return_valuer(return_arg)
        if case_return_arg is not None:
            return_arg = case_return_arg

        case_valuers = {}
        for case_value, field in cases_arg.items():
            case_valuers[case_value] = self.compile_valuer(field)
        value_valuer = self.compile_valuer(value_arg) if value_arg else None
        default_valuer = self.compile_valuer(default_arg) if default_arg else None
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "case_valuer",
            "key": key,
            "filter": filter,
            'value_valuer': value_valuer,
            "case_valuers": case_valuers,
            "default_valuer": default_valuer,
            "return_valuer": return_valuer,
        }

    def compile_calculate_valuer(self, key="", filter=None, args=None):
        args_valuers, return_arg = [], None
        if isinstance(args, list):
            for arg in args:
                arg_return_arg, _ = self.parse_return_valuer(arg)
                if arg_return_arg is not None:
                    return_arg = arg_return_arg
                else:
                    args_valuers.append(self.compile_valuer(arg))
        else:
            args_valuers.append(args)
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "calculate_valuer",
            "key": key,
            "filter": filter,
            "args_valuers": args_valuers,
            "return_valuer": return_valuer,
        }

    def compile_schema_valuer(self, schema=None):
        schema_valuers = {}

        if isinstance(schema, dict):
            for key, field in schema.items():
                schema_valuers[key] = self.compile_valuer(field)

        return {
            "name": "schema_valuer",
            "key": "",
            "schema_valuers": schema_valuers,
        }

    def compile_make_valuer(self, key="", filter=None, value_arg=None, return_arg=None):
        return_arg, _ = self.parse_return_valuer(return_arg)

        if isinstance(value_arg, dict):
            value_valuer = {key: (self.compile_valuer(key), self.compile_valuer(value))
                            for key, value in value_arg.items()}
        elif isinstance(value_arg, list):
            if len(value_arg) == 1:
                value_valuer = self.compile_valuer(value_arg[0])
            else:
                value_valuer = [self.compile_valuer(value) for value in value_arg]
        else:
            value_valuer = self.compile_valuer(value_arg)
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "make_valuer",
            "key": key,
            "filter": filter,
            "value_valuer": value_valuer,
            "return_valuer": return_valuer,
        }

    def compile_let_valuer(self, key="", filter=None, key_arg=None, return_arg=None):
        return_arg, _ = self.parse_return_valuer(return_arg)

        key_valuer = self.compile_valuer(key_arg)
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "let_valuer",
            "key": "",
            "filter": filter,
            "key_valuer": key_valuer,
            "return_valuer": return_valuer,
        }

    def compile_yield_valuer(self, key="", filter=None, value_arg=None, return_arg=None):
        return_arg, _ = self.parse_return_valuer(return_arg)

        value_valuer = self.compile_valuer(value_arg) if value_arg else None
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "yield_valuer",
            "key": "",
            "filter": filter,
            "value_valuer": value_valuer,
            "return_valuer": return_valuer,
        }

    def compile_aggregate_valuer(self, key="", filter=None, key_arg=None, calculate_arg=None):
        return_arg, _ = self.parse_return_valuer(calculate_arg)

        key_valuer = self.compile_valuer(key_arg)
        calculate_valuer = self.compile_valuer(calculate_arg if return_arg is None else return_arg)

        return {
            "name": "aggregate_valuer",
            "key": "",
            "filter": filter,
            "key_valuer": key_valuer,
            "calculate_valuer": calculate_valuer,
        }

    def compile_call_valuer(self, key="", filter=None, value_arg=None, calculate_arg=None, return_arg=None):
        return_arg, _ = self.parse_return_valuer(return_arg)

        value_valuer = self.compile_valuer(value_arg) if value_arg else None
        calculate_valuer = self.compile_valuer(calculate_arg)
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "call_valuer",
            "key": key,
            "filter": filter,
            "value_valuer": value_valuer,
            "calculate_valuer": calculate_valuer,
            "return_valuer": return_valuer,
        }

    def compile_assign_valuer(self, key="", filter=None, calculate_arg=None, return_arg=None):
        calculated_return_arg, calculate_arg = self.parse_return_valuer(calculate_arg)
        if return_arg:
            return_arg, _ = self.parse_return_valuer(return_arg)
        return_arg = return_arg or calculated_return_arg

        calculate_valuer = self.compile_valuer(calculate_arg) if calculate_arg else None
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "assign_valuer",
            "key": key,
            "filter": filter,
            "calculate_valuer": calculate_valuer,
            "return_valuer": return_valuer,
        }

    def compile_lambda_valuer(self, key="", filter=None, calculate_arg=None):
        calculate_valuer = self.compile_valuer(calculate_arg)

        return {
            "name": "lambda_valuer",
            "key": key,
            "filter": filter,
            "calculate_valuer": calculate_valuer,
        }

    def compile_foreach_valuer(self, key="", filter=None, value_arg=None, calculate_arg=None, return_arg=None):
        calculated_return_arg, calculate_arg = self.parse_return_valuer(calculate_arg)
        if return_arg:
            return_arg, _ = self.parse_return_valuer(return_arg)
        return_arg = return_arg or calculated_return_arg

        value_valuer = self.compile_valuer(value_arg) if value_arg else None
        calculate_valuer = self.compile_valuer(calculate_arg)
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "foreach_valuer",
            "key": key,
            "filter": filter,
            "value_valuer": value_valuer,
            "calculate_valuer": calculate_valuer,
            "return_valuer": return_valuer,
        }

    def compile_break_valuer(self, key="", filter=None, return_arg=None):
        return_arg, _ = self.parse_return_valuer(return_arg)

        return_valuer = self.compile_valuer(return_arg) if return_arg else None
        return {
            "name": "break_valuer",
            "key": key,
            "filter": filter,
            "return_valuer": return_valuer,
        }

    def compile_continue_valuer(self, key="", filter=None, return_arg=None):
        return_arg, _ = self.parse_return_valuer(return_arg)

        return_valuer = self.compile_valuer(return_arg) if return_arg else None
        return {
            "name": "continue_valuer",
            "key": key,
            "filter": filter,
            "return_valuer": return_valuer,
        }

    def compile_if_valuer(self, key="", filter=None, value_arg=None, true_arg=None, false_arg=None, return_arg=None):
        false_return_arg, false_arg = self.parse_return_valuer(false_arg)
        if return_arg:
            return_arg, _ = self.parse_return_valuer(return_arg)
        return_arg = return_arg or false_return_arg

        value_valuer = self.compile_valuer(value_arg) if value_arg else None
        true_valuer = self.compile_valuer(true_arg) if true_arg else None
        false_valuer = self.compile_valuer(false_arg) if false_arg else None
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "if_valuer",
            "key": key,
            "filter": filter,
            'value_valuer': value_valuer,
            "true_valuer": true_valuer,
            "false_valuer": false_valuer,
            "return_valuer": return_valuer,
        }

    def compile_match_valuer(self, key="", filter=None, value_arg=None, matchs_arg=None, default_arg=None, return_arg=None):
        match_return_arg, _ = self.parse_return_valuer(return_arg)
        if match_return_arg is not None:
            return_arg = match_return_arg

        match_valuers = {}
        for match_value, field in matchs_arg.items():
            match_valuers[match_value] = self.compile_valuer(field)
        value_valuer = self.compile_valuer(value_arg) if value_arg else None
        default_valuer = self.compile_valuer(default_arg) if default_arg else None
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "match_valuer",
            "key": key,
            "filter": filter,
            'value_valuer': value_valuer,
            "match_valuers": match_valuers,
            "default_valuer": default_valuer,
            "return_valuer": return_valuer,
        }

    def compile_state_valuer(self, key="", filter=None, calculate_arg=None, deafult_arg=None, return_arg=None):
        calculated_return_arg, calculate_arg = self.parse_return_valuer(calculate_arg)
        default_return_arg, deafult_arg = self.parse_return_valuer(deafult_arg)
        if return_arg:
            return_arg, _ = self.parse_return_valuer(return_arg)
        return_arg = return_arg or default_return_arg or calculated_return_arg

        calculate_valuer = self.compile_valuer(calculate_arg) if calculate_arg else None
        deafult_valuer = self.compile_valuer(deafult_arg) if deafult_arg else None
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "state_valuer",
            "key": key,
            "filter": filter,
            "calculate_valuer": calculate_valuer,
            "default_valuer": deafult_valuer,
            "return_valuer": return_valuer,
        }

    def compile_cache_valuer(self, key="", filter=None, key_arg=None, calculate_arg=None, return_arg=None):
        return_arg, _ = self.parse_return_valuer(return_arg)

        key_valuer = self.compile_valuer(key_arg) if key_arg else None
        calculate_valuer = self.compile_valuer(calculate_arg) if calculate_arg else None
        return_valuer = self.compile_valuer(return_arg) if return_arg else None

        return {
            "name": "cache_valuer",
            "key": key,
            "filter": filter,
            "key_valuer": key_valuer,
            "calculate_valuer": calculate_valuer,
            "return_valuer": return_valuer,
        }