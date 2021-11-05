# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import os

def get_rich():
    if os.environ.get("USE_RICH", 'true').lower() != "true":
        return None

    try:
        import rich
    except ImportError:
        return None
    return rich

def get_expression_name(expression):
    return {
        "==": "eq",
        "!=": "ne",
        ">": "gt",
        ">=": "gte",
        "<": "lt",
        "<=": "lte"
    }[expression]

def check_simple_object(value):
    if isinstance(value, dict):
        for k, v in value.items():
            if isinstance(k, (tuple, set, list, dict)):
                return False
            if isinstance(v, (tuple, set, list, dict)):
                return False
        return True
    if isinstance(value, (tuple, set, list)):
        for v in value:
            if isinstance(v, (tuple, set, list, dict)):
                return False
        return True
    return True

def print_object(value, indent="    ", deep=1):
    if isinstance(value, dict):
        if not value:
            return print("{}", end="")

        next_deep = 1 if len(value) > 4 or not check_simple_object(value) else 0
        print("{", end=("\n" if next_deep else ""))
        keys = sorted(list(value.keys()))
        for i in range(len(keys)):
            if next_deep:
                print(indent * deep, end="")
            print_object(keys[i], indent, deep + next_deep)
            print(": ", end="")
            print_object(value[keys[i]], indent, deep + next_deep)
            print(("," if next_deep else ", ") if i < len(keys) - 1 else "", end=("\n" if next_deep else ""))
        if next_deep:
            print(indent * (deep - 1) + "}", end="")
        else:
            print("}", end="")
    elif isinstance(value, (tuple, set, list)):
        next_deep = 1 if len(value) > 1 and not check_simple_object(value) else 0
        print("[", end="")
        for i in range(len(value)):
            if next_deep:
                print("\n" + indent * deep, end="")
                print_object(value[i], indent, deep + next_deep)
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