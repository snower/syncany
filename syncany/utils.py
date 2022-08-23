# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import os
import datetime
import random
import string
import pytz
from tzlocal import get_localzone

_runner_index = 0
def gen_runner_id():
    global _runner_index
    _runner_index += 1
    if _runner_index >= 100:
        _runner_index = 0
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S") + \
           "".join([random.choice(string.digits) for i in range(8)]) + ("%02d" % _runner_index)

_timezone = None
def get_timezone():
    global _timezone
    if _timezone is None:
        from .taskers.tasker import current_tasker
        tasker = current_tasker()
        if "options" in tasker.config and tasker.config["options"]:
            if "timezone" in tasker.config["options"]:
                _timezone = pytz.timezone(tasker.config["options"]["timezone"])
            else:
                _timezone = get_localzone()
        else:
            _timezone = get_localzone()
    return _timezone

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
        "<=": "lte",
        "in": "in"
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

class HumanRepr(object):
    def __init__(self, repr_value):
        self.repr_value = repr_value

    def __str__(self):
        return self.repr_value

    def __repr__(self):
        return self.repr_value

def human_format_object(value):
    if isinstance(value, dict):
        fvalues = {}
        for k, v in value.items():
            fvalues[k] = human_format_object(v)
        return fvalues
    if isinstance(value, (tuple, set, list)):
        fvalues = []
        for v in value:
            fvalues.append(human_format_object(v))
        return fvalues

    if isinstance(value, datetime.datetime):
        return HumanRepr('datetime.datetime("%s")' % value.isoformat())
    if isinstance(value, datetime.date):
        return HumanRepr('datetime.date("%s")' % value.isoformat())
    if isinstance(value, datetime.time):
        return HumanRepr('datetime.time("%s")' % value.isoformat())
    return value

def human_repr_object(value):
    if isinstance(value, dict):
        fvalues = []
        for k, v in value.items():
            fvalues.append("%s: %s" % (repr(k), human_repr_object(v)))
        return "{" + ", ".join(fvalues) + "}"
    if isinstance(value, (tuple, set, list)):
        fvalues = []
        for v in value:
            fvalues.append(human_repr_object(v))
        return "[" + ", ".join(fvalues) + "]"

    if isinstance(value, datetime.datetime):
        return 'datetime.datetime("%s")' % value.isoformat()
    if isinstance(value, datetime.date):
        return 'datetime.date("%s")' % value.isoformat()
    if isinstance(value, datetime.time):
        return 'datetime.time("%s")' % value.isoformat()
    return repr(value)