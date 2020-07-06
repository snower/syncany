# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import datetime
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None

class ValuerCompiler(object):
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

    def compile_db_valuer(self, key="", filter=None):
        return {
            "name": "db_valuer",
            "key": key,
            "filter": filter
        }

    def compile_inherit_valuer(self, key="", filter=None, reflen=0):
        return {
            "name": "inherit_valuer",
            "key": key,
            "filter": None,
            'reflen': reflen,
            "value_valuer": self.compile_db_valuer(key, filter)
        }

    def compile_db_join_valuer(self, key="", loader=None, foreign_key="", foreign_filters=None, filter=None, args_arg=None, return_arg=None):
        args_valuer = self.compile_schema_field(args_arg) if args_arg else None

        return_arg = "$.*" if return_arg is None else return_arg
        if isinstance(return_arg, str) and return_arg[:1] == ":":
            return_arg = return_arg[1:]
        if isinstance(return_arg, (list, tuple, set)) and return_arg and isinstance(return_arg[0], str):
            if return_arg[0] == ":":
                return_arg = list(return_arg)[1:]
            elif return_arg[0][:1] == ":":
                return_arg = list(return_arg)
                return_arg[0] = return_arg[0][1:]
        return_valuer = self.compile_schema_field(return_arg)

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
        case_valuers = {}
        if isinstance(cases_arg, (list, tuple, set)):
            cases_args = []
            for index in range(len(cases_arg)):
                if isinstance(cases_arg[index], str) and cases_arg[index][:1] == ":":
                    return_arg = cases_arg[index][1:]
                    continue
                if isinstance(cases_arg[index], (list, tuple, set)) and cases_arg[index] and isinstance(cases_arg[index][0], str):
                    if cases_arg[index][0] == ":":
                        return_arg = list(cases_arg[index])[1:]
                        continue
                    if cases_arg[index][0][:1] == ":":
                        return_arg = list(cases_arg[index])
                        return_arg[0] = return_arg[0][1:]
                        continue
                cases_args.append(cases_arg[index])

            if len(cases_args) == 3:
                value_arg = cases_args[0]
                case_valuers[0] = self.compile_schema_field(cases_args[1])
                case_valuers[1] = self.compile_schema_field(cases_args[2])
            elif len(cases_args) == 2:
                case_valuers[0] = self.compile_schema_field(cases_args[0])
                case_valuers[1] = self.compile_schema_field(cases_args[1])
        elif isinstance(cases_arg, dict):
            for case_value, field in cases_arg.items():
                case_valuers[case_value] = self.compile_schema_field(field)

        value_valuer = self.compile_schema_field(value_arg) if value_arg else None
        default_valuer = self.compile_schema_field(default_arg) if default_arg else None
        return_valuer = self.compile_schema_field(return_arg) if return_arg else None

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
        args_valuers, return_valuer = [], None
        if isinstance(args, list):
            for arg in args:
                if isinstance(arg, str) and arg[:1] == ":":
                    return_valuer = self.compile_schema_field(arg[1:])
                elif isinstance(arg, (list, tuple, set)) and arg and isinstance(arg[0], str):
                    if arg[0] == ":":
                        return_valuer = self.compile_schema_field(list(arg)[1:])
                    elif arg[0][:1] == ":":
                        arg = list(arg)
                        arg[0] = arg[0][1:]
                        return_valuer = self.compile_schema_field(arg)
                    else:
                        args_valuers.append(self.compile_schema_field(arg))
                else:
                    args_valuers.append(self.compile_schema_field(arg))
        else:
            args_valuers.append(args)

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
                schema_valuers[key] = self.compile_schema_field(field)

        return {
            "name": "schema_valuer",
            "key": "",
            "schema_valuers": schema_valuers,
        }

    def compile_make_valuer(self, key="", filter=None, valuer=None, loop_condition_returns=None):
        if isinstance(valuer, dict):
            valuer = {key: (self.compile_schema_field(key), self.compile_schema_field(value))
                      for key, value in valuer.items()}
        elif isinstance(valuer, (list, tuple, set)):
            valuer = [self.compile_schema_field(value) for value in valuer]
        else:
            valuer = self.compile_schema_field(valuer)

        loop, loop_valuer = None, None
        condition, condition_valuer, condition_break = None, None, None
        return_valuer = None
        for lcr in loop_condition_returns:
            if isinstance(lcr, str):
                if lcr[:4] == "#for":
                    loop = lcr
                elif lcr[:3] == "#if":
                    condition = lcr
                elif lcr[:1] == ":":
                    return_valuer = self.compile_schema_field(lcr[1:])
            elif lcr and isinstance(lcr, (list, tuple, set)):
                if lcr[0][:4] == "#for" and len(lcr) == 2:
                    loop, loop_valuer = lcr[0], self.compile_schema_field(lcr[1])
                elif lcr[0][:3] == "#if" and len(lcr) in (2, 3):
                    condition, condition_valuer, condition_break = lcr[0], self.compile_schema_field(lcr[1]),\
                                                                   lcr[2] if len(lcr) >= 3 else None
                elif lcr[0][:1] == ":":
                    if lcr[0] == ":":
                        lcr = list(lcr)[1:]
                    else:
                        lcr = list(lcr)
                        lcr[0] = lcr[0][1:]
                    return_valuer = self.compile_schema_field(lcr)

        return {
            "name": "make_valuer",
            "key": key,
            "filter": filter,
            "valuer": valuer,
            "loop": loop,
            "loop_valuer": loop_valuer,
            "condition": condition,
            "condition_valuer": condition_valuer,
            "condition_break": condition_break,
            "return_valuer": return_valuer,
        }

    def compile_let_valuer(self, key="", filter=None, key_arg=None, return_arg=None):
        key_valuer = self.compile_schema_field(key_arg)
        if isinstance(return_arg, str) and return_arg[:1] == ":":
            return_arg = return_arg[1:]
        elif isinstance(return_arg, (list, tuple, set)) and return_arg and isinstance(return_arg[0], str):
            if return_arg[0] == ":":
                return_arg = list(return_arg)[1:]
            elif return_arg[0][:1] == ":":
                return_arg = list(return_arg)
                return_arg[0] = return_arg[0][1:]
        return_valuer = self.compile_schema_field(return_arg) if return_arg else None

        return {
            "name": "let_valuer",
            "key": "",
            "filter": filter,
            "key_valuer": key_valuer,
            "return_valuer": return_valuer,
        }

    def compile_yield_valuer(self, key="", filter=None, value_arg=None, return_arg=None):
        value_valuer = self.compile_schema_field(value_arg) if value_arg else None
        if isinstance(return_arg, str) and return_arg[:1] == ":":
            return_arg = return_arg[1:]
        elif isinstance(return_arg, (list, tuple, set)) and return_arg and isinstance(return_arg[0], str):
            if return_arg[0] == ":":
                return_arg = list(return_arg)[1:]
            elif return_arg[0][:1] == ":":
                return_arg = list(return_arg)
                return_arg[0] = return_arg[0][1:]
        return_valuer = self.compile_schema_field(return_arg) if return_arg else None

        return {
            "name": "yield_valuer",
            "key": "",
            "filter": filter,
            "value_valuer": value_valuer,
            "return_valuer": return_valuer,
        }

    def compile_aggregate_valuer(self, key="", filter=None, key_arg=None, calculate_arg=None):
        key_valuer = self.compile_schema_field(key_arg)
        if isinstance(calculate_arg, str) and calculate_arg[:1] == ":":
            calculate_arg = calculate_arg[1:]
        elif isinstance(calculate_arg, (list, tuple, set)) and calculate_arg and isinstance(calculate_arg[0], str):
            if calculate_arg[0] == ":":
                calculate_arg = list(calculate_arg)[1:]
            elif calculate_arg[0][:1] == ":":
                calculate_arg = list(calculate_arg)
                calculate_arg[0] = calculate_arg[0][1:]
        calculate_valuer = self.compile_schema_field(calculate_arg)

        return {
            "name": "aggregate_valuer",
            "key": "",
            "filter": filter,
            "key_valuer": key_valuer,
            "calculate_valuer": calculate_valuer,
        }

    def compile_call_valuer(self, key="", filter=None, return_arg=None, calculate_arg=None):
        if isinstance(return_arg, str) and return_arg[:1] == ":":
            return_arg = return_arg[1:]
        elif isinstance(return_arg, (list, tuple, set)) and return_arg and isinstance(return_arg[0], str):
            if return_arg[0] == ":":
                return_arg = list(return_arg)[1:]
            elif return_arg[0][:1] == ":":
                return_arg = list(return_arg)
                return_arg[0] = return_arg[0][1:]

        return_valuer = self.compile_schema_field(return_arg) if return_arg else None
        calculate_valuer = self.compile_schema_field(calculate_arg)

        return {
            "name": "call_valuer",
            "key": key,
            "filter": filter,
            "return_valuer": return_valuer,
            "calculate_valuer": calculate_valuer,
        }

    def compile_assign_valuer(self, key="", filter=None, calculate_arg=None, return_arg=None):
        if isinstance(calculate_arg, str) and calculate_arg[:1] == ":":
            return_arg, calculate_arg = calculate_arg[1:], None
        elif isinstance(calculate_arg, (list, tuple, set)) and calculate_arg and isinstance(calculate_arg[0], str):
            if calculate_arg[0] == ":":
                return_arg, calculate_arg = list(calculate_arg)[1:], None
            elif calculate_arg[0][:1] == ":":
                return_arg, calculate_arg = list(calculate_arg), None
                return_arg[0] = return_arg[0][1:]

        if isinstance(return_arg, str) and return_arg[:1] == ":":
            return_arg = return_arg[1:]
        elif isinstance(return_arg, (list, tuple, set)) and return_arg and isinstance(return_arg[0], str):
            if return_arg[0] == ":":
                return_arg = list(return_arg)[1:]
            elif return_arg[0][:1] == ":":
                return_arg = list(return_arg)
                return_arg[0] = return_arg[0][1:]

        calculate_valuer = self.compile_schema_field(calculate_arg) if calculate_arg else None
        return_valuer = self.compile_schema_field(return_arg) if return_arg else None

        return {
            "name": "assign_valuer",
            "key": key,
            "filter": filter,
            "calculate_valuer": calculate_valuer,
            "return_valuer": return_valuer,
        }