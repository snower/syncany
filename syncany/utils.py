# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import os
import datetime
import random
import string
from decimal import Decimal
import pytz
from pendulum.parsing import parse as pendulum_parse
from pendulum.parsing.exceptions import ParserError
from tzlocal import get_localzone

NumberTypes = (int, float)
NumberDecimalTypes = (int, float, Decimal)
SequenceTypes = (tuple, list)


class CmpValue(object):
    def __init__(self, value, reverse=False):
        self.value = value
        self.reverse = reverse

    def format_value(self, other):
        if isinstance(self.value, datetime.date):
            if isinstance(self.value, datetime.datetime):
                return parse_datetime(other.value, None, get_timezone())
            return parse_date(other.value, None, get_timezone())
        if isinstance(self.value, datetime.time):
            return parse_time(other.value, None, get_timezone())
        type_cls = type(self.value)
        return type_cls(other.value)

    def __eq__(self, other):
        return self.value == other.value

    def __gt__(self, other):
        try:
            if self.reverse:
                return self.value < other.value
            return self.value > other.value
        except:
            if self.value is None:
                if other.value is None:
                    return False if self.reverse else True
                return True if self.reverse else False
            if other.value is None:
                return False if self.reverse else True
            try:
                if self.reverse:
                    return self.value < self.format_value(other)
                return self.value > self.format_value(other)
            except:
                return False if self.reverse else True

    def __lt__(self, other):
        try:
            if self.reverse:
                return self.value > other.value
            return self.value < other.value
        except:
            if self.value is None:
                if other.value is None:
                    return True if self.reverse else False
                return False if self.reverse else True
            if other.value is None:
                return True if self.reverse else False
            try:
                if self.reverse:
                    return self.value > self.format_value(other)
                return self.value < self.format_value(other)
            except:
                return True if self.reverse else False

    def __ne__(self, other):
        return self.value != other.value

def sorted_by_keys(iterable, keys=None, reverse=None):
    if not keys:
        return sorted(iterable, reverse=True if reverse else False)
    if not isinstance(keys, SequenceTypes):
        keys = [keys]
    reverse_keys = [key for key in keys if isinstance(key, SequenceTypes) and len(key) == 2
                    and isinstance(key[0], str) and key[1]]
    if reverse is None:
        reverse = True if len(reverse_keys) > len(keys) / 2 else False
    else:
        reverse = True if reverse else False
    sort_keys = []
    for key in keys:
        if isinstance(key, str):
            sort_key = (key.split("."), True if reverse else False)
        elif isinstance(key, SequenceTypes) and len(key) == 2 and isinstance(key[0], str):
            sort_key = (key[0].split("."), (False if key[1] else True) if reverse else (True if key[1] else False))
        else:
            raise TypeError("unknown keys type: " + str(keys))
        sort_keys.append(sort_key)
    if len(sort_keys) == 1 and len(sort_keys[0][0]) == 1:
        sort_key = sort_keys[0][0][0]
        reverse = (not sort_keys[0][1]) if reverse else sort_keys[0][1]
        try:
            return sorted(iterable, key=lambda x: x[sort_key], reverse=reverse)
        except:
            return sorted(iterable, key=lambda x: CmpValue(x[sort_key]), reverse=reverse)

    def get_cmp_key(x):
        key_values = []
        for ks, kr in sort_keys:
            key_value = None
            for k in ks:
                key_value = x[k]
            if not kr:
                key_values.append(key_value)
            else:
                key_values.append(-key_value if isinstance(key_value, NumberTypes) else CmpValue(key_value, True))
        return tuple(key_values)
    try:
        return sorted(iterable, key=get_cmp_key, reverse=reverse)
    except:
        def get_format_cmp_key(x):
            key_values = []
            for ks, kr in sort_keys:
                key_value = None
                for k in ks:
                    key_value = x[k]
                key_values.append(CmpValue(key_value, False if not kr else True))
            return tuple(key_values)
        return sorted(iterable, key=get_format_cmp_key, reverse=reverse)

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
        if tasker and "options" in tasker.config and tasker.config["options"]:
            if "timezone" in tasker.config["options"]:
                _timezone = pytz.timezone(tasker.config["options"]["timezone"])
            else:
                _timezone = get_localzone()
        else:
            _timezone = get_localzone()
    return _timezone

def set_timezone(timezone):
    global _timezone
    _timezone = timezone

def ensure_timezone(dt):
    tz = get_timezone() if not _timezone else _timezone
    try:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=tz)
        if dt.tzinfo != tz:
            return dt.astimezone(tz=tz)
    except:
        return dt
    return dt

def parse_datetime(value, fmt, tz):
    try:
        dt = pendulum_parse(value)
    except ParserError:
        try:
            if "." in value:
                if len(value.split(".")[0]) == 6:
                    dt = datetime.datetime.strptime(value, "%H%M%S.%f")
                    now = datetime.datetime.now()
                    return datetime.datetime(now.year, now.month, now.day, dt.hour, dt.minute, dt.second,
                                             dt.microsecond, tzinfo=tz)
                dt = datetime.datetime.strptime(value, "%Y%m%d%H%M%S.%f")
            elif len(value) == 8:
                dt = datetime.datetime.strptime(value, "%Y%m%d%H%M%S.%f")
            elif len(value) == 6:
                dt = datetime.datetime.strptime(value, "%H%M%S")
                now = datetime.datetime.now()
                return datetime.datetime(now.year, now.month, now.day, dt.hour, dt.minute, dt.second, dt.microsecond,
                                         tzinfo=tz)
            else:
                dt = datetime.datetime.strptime(value, "%Y%m%d%H%M%S")
        except:
            return datetime.datetime.strptime(value, fmt or "%Y-%m-%d %H:%M:%S")

    if isinstance(dt, datetime.datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        elif tz != dt.tzinfo:
            dt = dt.astimezone(tz=tz)
        return dt
    if isinstance(dt, datetime.date):
        return datetime.datetime(dt.year, dt.month, dt.day, tzinfo=tz)
    if isinstance(dt, datetime.time):
        now = datetime.datetime.now()
        return datetime.datetime(now.year, now.month, now.day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=tz)
    return datetime.datetime.strptime(value, fmt or "%Y-%m-%d %H:%M:%S")

def parse_date(value, fmt, tz):
    try:
        dt = parse_datetime(value, fmt, tz)
        if isinstance(dt, datetime.datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
            elif tz != dt.tzinfo:
                dt = dt.astimezone(tz=tz)
            return datetime.date(dt.year, dt.month, dt.day)
        if isinstance(dt, datetime.date):
            return dt
    except ParserError:
        pass
    dt = datetime.datetime.strptime(value, fmt or "%Y-%m-%d")
    return datetime.date(dt.year, dt.month, dt.day)

def parse_time(value, fmt, tz):
    try:
        dt = parse_datetime(value, fmt, tz)
        if isinstance(dt, datetime.time):
            return dt
        if isinstance(dt, datetime.datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
            elif tz != dt.tzinfo:
                dt = dt.astimezone(tz=tz)
            return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=tz)
    except ParserError:
        pass
    dt = datetime.datetime.strptime("2000-01-01 " + value, "%Y-%m-%d " + (fmt or "%H:%M:%S"))
    return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=tz)

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
            if isinstance(k, SequenceTypes) or isinstance(k, dict):
                return False
            if isinstance(v, SequenceTypes) or isinstance(v, dict):
                return False
        return True
    if isinstance(value, SequenceTypes):
        for v in value:
            if isinstance(v, SequenceTypes) or isinstance(v, dict):
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
    elif isinstance(value, SequenceTypes):
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
    if isinstance(value, SequenceTypes):
        fvalues = []
        for v in value:
            fvalues.append(human_format_object(v))
        return fvalues

    if isinstance(value, datetime.date):
        if isinstance(value, datetime.datetime):
            return HumanRepr('datetime.datetime("%s")' % value.isoformat())
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
    if isinstance(value, SequenceTypes):
        fvalues, require_newline = [], any([isinstance(v, dict) for v in list(value)[:10]])
        for v in list(value)[:10]:
            fvalues.append(human_repr_object(v))
        if len(value) > 10:
            fvalues.append("...(%d)" % len(value))
        if require_newline:
            return "[\n    " + ",\n    ".join(fvalues) + "\n]"
        return "[" + ", ".join(fvalues) + "]"

    if isinstance(value, datetime.date):
        if isinstance(value, datetime.datetime):
            return 'datetime.datetime("%s")' % value.isoformat()
        return 'datetime.date("%s")' % value.isoformat()
    if isinstance(value, datetime.time):
        return 'datetime.time("%s")' % value.isoformat()
    return repr(value)