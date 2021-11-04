# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

def get_expression_name(expression):
    return {
        "==": "eq",
        "!=": "ne",
        ">": "gt",
        ">=": "gte",
        "<": "lt",
        "<=": "lte"
    }[expression]

def print_object(value, indent="    ", deep=1):
    if isinstance(value, dict):
        if not value:
            return print("{}", end="")

        print("{")
        keys = sorted(list(value.keys()))
        for i in range(len(keys)):
            print(indent * deep, end="")
            print_object(keys[i], indent, deep + 1)
            print(": ", end="")
            print_object(value[keys[i]], indent, deep + 1)
            print("," if i < len(keys) - 1 else "")
        print(indent * (deep - 1) + "}", end="")
    elif isinstance(value, (tuple, set, list)):
        print("[", end="")
        for i in range(len(value)):
            if isinstance(value[i], dict):
                print("\n" + indent * deep, end="")
                print_object(value[i], indent, deep + 1)
                print(("," + indent * deep) if i < len(value) - 1 else ("\n" + indent * (deep - 1)), end="")
            else:
                print_object(value[i], indent, deep)
                if i < len(value) - 1:
                    print(", ", end="")
        print("]", end="")
    elif isinstance(value, str):
        print('"%s"' % value, end="")
    else:
        print(value, end="")

    if deep == 1:
        print()