# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import struct
import math
import hashlib
import datetime
import pytz
import json
import re
from ..utils import get_timezone, sorted_by_keys
from .calculater import Calculater
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None


class TypeCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if self.args[0] is None:
            return "null"

        if self.args[0] is True or self.args[0] is False:
            return "boolean"

        if isinstance(self.args[0], dict):
            return "map"

        if isinstance(self.args[0], (list, tuple, set)):
            return "array"

        if isinstance(self.args[0], int):
            return "int"

        if isinstance(self.args[0], float):
            return "float"

        if ObjectId and isinstance(self.args[0], ObjectId):
            return "objectid"

        return type(self.args[0]).__module__ + "." + type(self.args[0]).__name__


class RangeCalculater(Calculater):
    def calculate(self):
        return range(*tuple(self.args))


class AddCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str):
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[1] in data:
                    if callable(self.args[2]):
                        data[self.args[1]] = data[self.args[1]] + self.args[2](data)
                    else:
                        data[self.args[1]] = data[self.args[1]] + self.args[2]
            return self.args[0]

        if len(self.args) >= 2:
            if isinstance(self.args[0], datetime.datetime) and isinstance(self.args[1], (int, float)):
                return self.args[0] + datetime.timedelta(seconds=int(self.args[1]))

            if isinstance(self.args[0], datetime.date) and isinstance(self.args[1], (int, float)):
                return self.args[0] + datetime.timedelta(days=int(self.args[1]))

        result = None
        for value in self.args:
            if value is None:
                continue
            if result is None:
                result = self.format_type(value)
            else:
                result += self.format_type(value)
        return result


class SubCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str):
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[1] in data:
                    if callable(self.args[2]):
                        data[self.args[1]] = data[self.args[1]] - self.args[2](data)
                    else:
                        data[self.args[1]] = data[self.args[1]] - self.args[2]
            return self.args[0]

        if len(self.args) >= 2:
            if isinstance(self.args[0], datetime.datetime) and isinstance(self.args[1], (int, float)):
                return self.args[0] - datetime.timedelta(seconds=int(self.args[1]))

            if isinstance(self.args[0], datetime.date) and isinstance(self.args[1], (int, float)):
                return self.args[0] - datetime.timedelta(days=int(self.args[1]))

        result = None
        for value in self.args:
            if value is None:
                continue
            if result is None:
                result = self.format_type(value)
            else:
                result -= self.format_type(value)
        return result


class MulCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str):
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[1] in data:
                    if callable(self.args[2]):
                        data[self.args[1]] = data[self.args[1]] * self.args[2](data)
                    else:
                        data[self.args[1]] = data[self.args[1]] * self.args[2]
            return self.args[0]

        result = None
        for value in self.args:
            if value is None:
                continue
            if result is None:
                result = self.format_type(value)
            else:
                result *= self.format_type(value)
        return result


class DivCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str):
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[1] in data:
                    if callable(self.args[2]):
                        data[self.args[1]] = data[self.args[1]] / self.args[2](data)
                    else:
                        data[self.args[1]] = data[self.args[1]] / self.args[2]
            return self.args[0]

        result = None
        for value in self.args:
            if value is None:
                continue
            if result is None:
                result = self.format_type(value)
            else:
                result /= self.format_type(value)
        return result


class ModCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str):
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[1] in data:
                    if callable(self.args[2]):
                        data[self.args[1]] = data[self.args[1]] % self.args[2](data)
                    else:
                        data[self.args[1]] = data[self.args[1]] % self.args[2]
            return self.args[0]

        result = None
        for value in self.args:
            if value is None:
                continue
            if result is None:
                result = self.format_type(value)
            else:
                result = result % self.format_type(value)
        return 0 if result is None else result


class BitCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) in (3, 4) and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str) and isinstance(self.args[2], str):
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[1] in data:
                    if self.args[2] == ">>":
                        data[self.args[1]] = data[self.args[1]] >> self.args[3]
                    if self.args[2] == "<<":
                        data[self.args[1]] = data[self.args[1]] << self.args[3]
                    if self.args[2] == "&":
                        data[self.args[1]] = data[self.args[1]] & self.args[3]
                    if self.args[2] == "|":
                        data[self.args[1]] = data[self.args[1]] | self.args[3]
                    if self.args[2] == "^":
                        data[self.args[1]] = data[self.args[1]] ^ self.args[3]
                    if self.args[2] == "~":
                        data[self.args[1]] = ~ data[self.args[1]]
            return self.args[0]

        if len(self.args) == 3:
            if self.args[0] is None:
                return 0
            if self.args[2] is None:
                return self.args[0]
            if self.args[1] == ">>":
                return self.format_type(self.args[0]) >> self.format_type(self.args[2])
            if self.args[1] == "<<":
                return self.format_type(self.args[0]) << self.format_type(self.args[2])
            if self.args[1] == "&":
                return self.format_type(self.args[0]) & self.format_type(self.args[2])
            if self.args[1] == "|":
                return self.format_type(self.args[0]) | self.format_type(self.args[2])
            if self.args[1] == "^":
                return self.format_type(self.args[0]) ^ self.format_type(self.args[2])

        if len(self.args) == 2:
            if self.args[1] is None:
                return 0
            if self.args[0] == "~":
                return ~ self.args[1]
        return 0


class SubstringCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return ""

        if len(self.args) in (3, 4) and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str):
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[1] in data:
                    if len(self.args) == 4:
                        data[self.args[1]] = data[self.args[1]][self.args[2]: self.args[2] + self.args[3]]
                    else:
                        data[self.args[1]] = data[self.args[1]][self.args[2]:]
            return self.args[0]

        if len(self.args) >= 3:
            if self.args[2] < 0:
                return self.args[0][self.args[1]: self.args[2]]
            return self.args[0][self.args[1]: self.args[1] + self.args[2]]

        if len(self.args) >= 2:
            return self.args[0][self.args[1]:]

        return self.args[0]


class SplitCalculater(Calculater):
    def split(self, data, split_key):
        if isinstance(data, str):
            return data.split(split_key)
        if isinstance(data, bytes):
            return data.decode("utf-8").split(split_key)
        if isinstance(data, list):
            result = []
            for cdata in data:
                result.extend(self.split(cdata, split_key))
            return result
        if isinstance(data, dict):
            result = []
            for key, value in data.items():
                result.extend(self.split(key, split_key))
                result.extend(self.split(value, split_key))
            return result
        return str(data).split(split_key)

    def calculate(self):
        if not self.args:
            return []

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str) and isinstance(self.args[2], str):
            result = []
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[2] in data:
                    result.extend(self.split(data[self.args[2]], self.args[1]))
            return result

        if len(self.args) == 2 and isinstance(self.args[0], list) and isinstance(self.args[1], str):
            return self.split(self.args[0], self.args[1])

        return self.split(self.args[1:], self.args[0])


class JoinCalculater(Calculater):
    def join_extend(self, data):
        if isinstance(data, str):
            return [data]
        if isinstance(data, bytes):
            return [data.decode("utf-8")]
        if isinstance(data, list):
            result = []
            for cdata in data:
                result.extend(self.join_extend(cdata))
            return result
        if isinstance(data, dict):
            result = []
            for key, value in data.items():
                result.extend(self.join_extend(key))
                result.extend(self.join_extend(value))
            return result
        return [str(data)]

    def calculate(self):
        if not self.args:
            return []

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str) and isinstance(self.args[2], str):
            result = []
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[2] in data:
                    result.extend(self.join_extend(data[self.args[2]]))
            return self.args[1].join(result)

        if len(self.args) == 2 and isinstance(self.args[0], list) and isinstance(self.args[1], str):
            return self.args[1].join(self.join_extend(self.args[0]))

        return self.args[0].join(self.join_extend(self.args[1:]))


class NowCalculater(Calculater):
    TIMEDELTAS = {"Y": 365 * 24 * 60 * 60, "m": 30 * 24 * 60 * 60, "d": 24 * 60 * 60, "H": 60 * 60, "M": 60, "S": 1}

    def calculate(self):
        if not self.args:
            return datetime.datetime.now(tz=get_timezone())

        if isinstance(self.args[0], int):
            return self.at(datetime.datetime.now(tz=get_timezone()), *tuple(self.args))
        if not self.args[0]:
            return datetime.datetime.now(tz=get_timezone())
        if self.args[0][0] in ("-", "+") and self.args[0][-1] in ("Y", "m", "d", "H", "M", "S"):
            try:
                seconds = int(self.args[0][1:-1]) * self.TIMEDELTAS[self.args[0][-1]]
            except:
                if len(self.args) >= 2:
                    return self.at(datetime.datetime.now(tz=pytz.timezone(self.args[0])), *tuple(self.args[1:]))
                return datetime.datetime.now(tz=pytz.timezone(self.args[0]))

            if len(self.args) >= 3:
                now = self.at(datetime.datetime.now(tz=pytz.timezone(self.args[1])), *tuple(self.args[2:]))
            elif len(self.args) >= 2:
                if isinstance(self.args[1], int):
                    now = self.at(datetime.datetime.now(tz=get_timezone()), *tuple(self.args[1:]))
                else:
                    now = datetime.datetime.now(tz=pytz.timezone(self.args[1]))
            else:
                now = datetime.datetime.now(tz=get_timezone())

            if self.args[0][0] == "-":
                return now - datetime.timedelta(seconds=seconds)
            return now + datetime.timedelta(seconds=seconds)
        if len(self.args) >= 2:
            return self.at(datetime.datetime.now(tz=pytz.timezone(self.args[0])), *tuple(self.args[1:]))
        return datetime.datetime.now(tz=pytz.timezone(self.args[0]))

    def at(self, dt, hour=0, minute=0, second=0, microsecond=0):
        return datetime.datetime(dt.year, dt.month, dt.day,
                                 hour if hour is not None else dt.hour, minute if minute is not None else dt.minute,
                                 second if second is not None else dt.second, microsecond if microsecond is not None else dt.microsecond,
                                 tzinfo=dt.tzinfo)



class EmptyCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return True

        if len(self.args) == 2:
            if isinstance(self.args[0], list) and isinstance(self.args[1], str):
                for data in self.args[0]:
                    if isinstance(data, dict) and self.args[1] in data:
                        if data[self.args[1]]:
                            return False
                return True
            elif isinstance(self.args[0], dict) and isinstance(self.args[1], str) and self.args[1] in self.args[0]:
                return False if self.args[0][self.args[1]] else True

        for data in self.args:
            if data:
                return False
        return True


class GtCalculater(Calculater):
    def cmp(self, left_value, right_value):
        if not left_value and not right_value:
            return False
        if left_value and not right_value:
            return True
        if not left_value and right_value:
            return False
        return left_value > right_value

    def calculate(self):
        if not self.args:
            return False

        left_value, right_value = self.format_type(self.args[0]), None
        for value in self.args[1:]:
            right_value = self.format_type(value)
            if not self.cmp(left_value, right_value):
                return False
            left_value, right_value = right_value, None
        return True


class GteCalculater(Calculater):
    def cmp(self, left_value, right_value):
        if not left_value and not right_value:
            return True
        if left_value and not right_value:
            return True
        if not left_value and right_value:
            return False
        return left_value >= right_value

    def calculate(self):
        if not self.args:
            return False

        left_value, right_value = self.format_type(self.args[0]), None
        for value in self.args[1:]:
            right_value = self.format_type(value)
            if not self.cmp(left_value, right_value):
                return False
            left_value, right_value = right_value, None
        return True


class LtCalculater(Calculater):
    def cmp(self, left_value, right_value):
        if not left_value and not right_value:
            return False
        if left_value and not right_value:
            return False
        if not left_value and right_value:
            return True
        return left_value < right_value

    def calculate(self):
        if not self.args:
            return False

        left_value, right_value = self.format_type(self.args[0]), None
        for value in self.args[1:]:
            right_value = self.format_type(value)
            if not self.cmp(left_value, right_value):
                return False
            left_value, right_value = right_value, None
        return True


class LteCalculater(Calculater):
    def cmp(self, left_value, right_value):
        if not left_value and not right_value:
            return True
        if left_value and not right_value:
            return False
        if not left_value and right_value:
            return True
        return left_value <= right_value

    def calculate(self):
        if not self.args:
            return False

        left_value, right_value = self.format_type(self.args[0]), None
        for value in self.args[1:]:
            right_value = self.format_type(value)
            if not self.cmp(left_value, right_value):
                return False
            left_value, right_value = right_value, None
        return True


class EqCalculater(Calculater):
    def cmp(self, left_value, right_value):
        if not left_value and not right_value:
            return True
        if left_value and not right_value:
            return False
        if not left_value and right_value:
            return False
        return left_value == right_value

    def calculate(self):
        if not self.args:
            return False

        left_value, right_value = self.format_type(self.args[0]), None
        for value in self.args[1:]:
            right_value = self.format_type(value)
            if not self.cmp(left_value, right_value):
                return False
            left_value, right_value = right_value, None
        return True


class NeqCalculater(Calculater):
    def cmp(self, left_value, right_value):
        if not left_value and not right_value:
            return False
        if left_value and not right_value:
            return True
        if not left_value and right_value:
            return True
        return left_value != right_value

    def calculate(self):
        if not self.args:
            return False

        left_value, right_value = self.format_type(self.args[0]), None
        for value in self.args[1:]:
            right_value = self.format_type(value)
            if not self.cmp(left_value, right_value):
                return False
            left_value, right_value = right_value, None
        return True


class AndCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        datas = self.args
        if len(self.args) == 2:
            if isinstance(self.args[0], list) and isinstance(self.args[1], str):
                datas = []
                for data in self.args[0]:
                    if isinstance(data, dict) and self.args[1] in data:
                        datas.append(data[self.args[1]])
            elif isinstance(self.args[0], dict) and isinstance(self.args[1], str) and self.args[1] in self.args[0]:
                datas = [self.args[0][self.args[1]]]

        result = datas[0]
        for i in range(1, len(datas)):
            result = result and datas[i]
        return result


class OrCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        datas = self.args
        if len(self.args) == 2:
            if isinstance(self.args[0], list) and isinstance(self.args[1], str):
                datas = []
                for data in self.args[0]:
                    if isinstance(data, dict) and self.args[1] in data:
                        datas.append(data[self.args[1]])
            elif isinstance(self.args[0], dict) and isinstance(self.args[1], str) and self.args[1] in self.args[0]:
                datas = [self.args[0][self.args[1]]]

        result = datas[0]
        for i in range(1, len(datas)):
            result = result or datas[i]
        return result


class InCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        if len(self.args) == 3 and isinstance(self.args[0], list) and isinstance(self.args[2], str):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[2] in data:
                    datas.append(data[self.args[2]])
            return self.args[1] in datas

        if len(self.args) == 2 and isinstance(self.args[0], list):
            return self.args[1] in self.args[0]

        try:
            result = self.args[0]
            for i in range(1, len(self.args)):
                if result not in self.args[i]:
                    return False
        except:
            return False
        return True


class MaxCalculater(Calculater):
    def max(self, values):
        result = None
        for value in values:
            if value is None:
                continue
            if result is None:
                result = self.format_type(value)
                continue
            value = self.format_type(value)
            if value > result:
                result = value
        return result

    def calculate(self):
        if not self.args:
            return None

        if not isinstance(self.args[0], list):
            if len(self.args) > 1:
                return self.max(self.args)
            return self.args[0]

        if len(self.args) == 2 and isinstance(self.args[1], str):
            if len(self.args[0]) == 1:
                return self.args[0][0]

            datas = {}
            for d in self.args[0]:
                if isinstance(d, dict) and self.args[1] in d:
                    datas[d[self.args[1]]] = d
            if not datas:
                return None
            max_key = max(*tuple(datas.keys()))
            return datas[max_key]

        max_key_value = max(*tuple(self.args[0]))
        if len(self.args) >= 2:
            if isinstance(self.args[1], list):
                try:
                    return self.args[1][self.args[0].index(max_key_value)]
                except:
                    return None

            if isinstance(self.args[1], dict):
                return self.args[1].get(max_key_value)
            return self.max([max_key_value] + list(self.args[1:]))
        return max_key_value


class MinCalculater(Calculater):
    def min(self, values):
        result = None
        for value in values:
            if value is None:
                continue
            if result is None:
                result = self.format_type(value)
                continue
            value = self.format_type(value)
            if value < result:
                result = value
        return result

    def calculate(self):
        if not self.args:
            return None

        if not isinstance(self.args[0], list):
            if len(self.args) >= 1:
                return self.min(self.args)
            return self.args[0]

        if len(self.args) == 2 and isinstance(self.args[1], str):
            if len(self.args[0]) == 1:
                return self.args[0][0]

            datas = {}
            for d in self.args[0]:
                if isinstance(d, dict) and self.args[1] in d:
                    datas[d[self.args[1]]] = d
            if not datas:
                return None
            min_key = min(*tuple(datas.keys()))
            return datas[min_key]

        min_key_value = min(*tuple(self.args[0]))
        if len(self.args) >= 2:
            if isinstance(self.args[1], list):
                try:
                    return self.args[1][self.args[0].index(min_key_value)]
                except:
                    return None

            if isinstance(self.args[1], dict):
                return self.args[1].get(min_key_value)
            return self.min([min_key_value] + list(self.args[1:]))
        return min_key_value


class LenCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) == 1:
            return len(self.args[0])
        return len(self.args)


class AbsCalculater(Calculater):
    def abs(self, arg):
        if isinstance(arg, (int, float)):
            return abs(arg)
        if isinstance(arg, str):
            try:

                return abs(float(arg) if "." in arg else int(arg))
            except:
                return 0
        if isinstance(arg, list):
            return [self.abs(child_arg) for child_arg in arg]
        if isinstance(arg, dict):
            return {child_key: self.abs(child_arg) for child_key, child_arg in arg.items()}
        return 0

    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) == 2 and isinstance(self.args[0], list) and isinstance(self.args[1], str):
            return [self.abs(data[self.args[1]]) for data in self.args[0] if isinstance(data, dict) and self.args[1] in data]

        result = []
        for arg in self.args:
            result.append(self.abs(arg))

        if len(result) == 1:
            return result[0]

        return result


class IndexCalculater(Calculater):
    def calculate(self):
        if not self.args or len(self.args) < 2:
            return None

        if isinstance(self.args[0], list):
            for data in self.args[0]:
                if len(self.args) >= 3:
                    if not isinstance(data, dict) or self.args[2] not in data:
                        continue
                    if data[self.args[2]] == self.args[1]:
                        return data
                elif data == self.args[1]:
                    return data

            if isinstance(self.args[1], (int, float)):
                return self.args[0][int(self.args[1])] if len(self.args[0]) < self.args[1] else None

        elif isinstance(self.args[0], dict):
            if len(self.args) >= 3:
                if self.args[2] not in self.args[0]:
                    return None

                if self.args[0][self.args[2]] == self.args[1]:
                    return self.args[0][self.args[2]]

            if self.args[1] in self.args[0]:
                return self.args[0][self.args[1]]

        return None


class FilterCalculater(Calculater):
    def calculate(self):
        if not self.args or len(self.args) < 2:
            return []

        result = []
        if isinstance(self.args[0], list):
            for data in self.args[0]:
                if len(self.args) >= 3:
                    if not isinstance(data, dict) or self.args[2] not in data:
                        continue
                    if data[self.args[2]] == self.args[1]:
                        result.append(data)
                elif data == self.args[1]:
                    result.append(data)

        elif isinstance(self.args[0], dict):
            if len(self.args) >= 3:
                if self.args[2] in self.args[0] and self.args[0][self.args[2]] == self.args[1]:
                    result.append(self.args[0])
            elif self.args[1] == self.args[0]:
                result.append(self.args[0])

        elif self.args[1] == self.args[0]:
            result.append(self.args[0])
        return result


class SumCalculater(Calculater):
    def add(self, v):
        if isinstance(v, (int, float)):
            return v
        elif v is True:
            return 1
        elif isinstance(v, str):
            try:
                return float(v)
            except:
                pass
        return 0

    def calculate(self):
        if not self.args:
            return 0

        result = 0
        if isinstance(self.args[0], list):
            for data in self.args[0]:
                if isinstance(data, dict) and len(self.args) >= 2:
                    if self.args[1] not in data:
                        continue
                    result += self.add(data[self.args[1]])
                else:
                    result += self.add(data)
        elif isinstance(self.args[0], dict):
            if len(self.args) >= 2:
                if self.args[1] in self.args[0]:
                    result += self.add(self.args[0][self.args[1]])
            else:
                result += self.add(self.args[0])
        else:
            result += self.add(self.args[0])
        return result


class SortCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        if not isinstance(self.args[0], list):
            return self.args[0]
        if len(self.args) == 2 and not isinstance(self.args[1], bool):
            return sorted_by_keys(self.args[0], keys=self.args[1], reverse=False)

        keys = self.args[2] if len(self.args) >= 3 else None
        return sorted_by_keys(self.args[0], keys=keys,
                              reverse=True if len(self.args) >= 2 and self.args[1] else False)


class StringCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return ''

        func_name = self.name[8:]
        if isinstance(self.args[0], str):
            if func_name == "contains":
                for cs in self.args[1:]:
                    if not isinstance(cs, str) or cs not in self.args[0]:
                        return False
                return True
            if hasattr(self.args[0], func_name):
                try:
                    return getattr(self.args[0], func_name)(*tuple(self.args[1:]))
                except:
                    return ''
        return ''


class ArrayCalculater(Calculater):
    def to_map(self):
        if len(self.args) == 2 and isinstance(self.args[0], list) and isinstance(self.args[1], str):
            result = {}
            for v in self.args[0]:
                if not isinstance(v, dict) or self.args[1] not in v:
                    continue
                vk = v[self.args[1]]
                if vk in result:
                    if not isinstance(result[vk], list):
                        result[vk] = [result[vk], v]
                    else:
                        result[vk].append(v)
                else:
                    result[vk] = v
            return result
        
        if len(self.args) == 1 and isinstance(self.args[0], list):
            if len(self.args[0]) == 1 and isinstance(self.args[0][0], dict):
                return self.args[0]
            return {"index" + str(i): self.args[0][i] for i in range(len(self.args[0]))}
        
        if isinstance(self.args[0], dict):
            if len(self.args) == 2 and isinstance(self.args[1], str):
                if self.args[1] in self.args[0]:
                    return {self.args[0][self.args[1]]: self.args[0]}
                return {}
            return self.args[0]
        return {}

    def flat(self):
        if len(self.args) == 1:
            if isinstance(self.args[0], list):
                result = []
                for d in self.args[0]:
                    if isinstance(d, list):
                        result.extend(d)
                    else:
                        result.append(d)
                return result
            return [self.args[0]]

        result = []
        for d in self.args:
            if isinstance(d, list):
                result.extend(d)
            else:
                result.append(d)
        return result

    def calculate(self):
        if not self.args:
            return None

        func_name = self.name[7:]
        if isinstance(self.args[0], list):
            if func_name == "map":
                return self.to_map()
            if func_name == "flat":
                return self.flat()
            if func_name == "contains":
                for cs in self.args[1:]:
                    if cs not in self.args[0]:
                        return False
                return True

            value = list(self.args[0])
            if hasattr(value, func_name):
                try:
                    result = getattr(value, func_name)(*tuple(self.args[1:]))
                    if func_name in ("append", "clear", "extend", "insert", "reverse", "sort"):
                        return value
                    return result
                except:
                    return None
        return None


class MapCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        func_name = self.name[5:]
        if isinstance(self.args[0], dict) and hasattr(self.args[0], func_name):
            try:
                result = getattr(self.args[0], func_name)(*tuple(self.args[1:]))
                if func_name in ("clear", "update"):
                    return self.args[0]
                if func_name == "contains":
                    for cs in self.args[1:]:
                        if cs not in self.args[0]:
                            return False
                    return True
                return result
            except:
                return None
        return None


class MathCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        func_name = self.name[6:]
        if hasattr(math, func_name):
            try:
                return getattr(math, func_name)(*tuple(self.args))
            except:
                return 0
        return 0


class HashCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        b = b''
        for arg in self.args:
            b += (arg if isinstance(arg, bytes) else str(arg).encode("utf-8"))

        if self.name == "hash::md5":
            return hashlib.md5(b).hexdigest()
        if self.name == "hash::sha1":
            return hashlib.sha1(b).hexdigest()
        if self.name == "hash::sha256":
            return hashlib.sha256(b).hexdigest()
        if self.name == "hash::sha384":
            return hashlib.sha384(b).hexdigest()
        if self.name == "hash::sha512":
            return hashlib.sha512(b).hexdigest()
        return None


class JsonCalculater(Calculater):
    def calculate(self):
        if self.name == "json::encode":
            return self.encode()
        if self.name == "json::decode":
            return self.decode()
        return None

    def encode(self):
        if not self.args:
            return None
        try:
            return json.dumps(self.args[0], default=str, ensure_ascii=False)
        except:
            return None

    def decode(self):
        if not self.args:
            return None
        try:
            return json.loads(self.args[0])
        except:
            return None


class StructCalculater(Calculater):
    def calculate(self):
        if self.name == "struct::pack":
            return self.pack()
        if self.name == "json::unpack":
            return self.unpack()
        return None

    def pack(self):
        if len(self.args) < 2:
            return None
        try:
            return struct.pack(self.args[0], self.args[1])
        except:
            return None

    def unpack(self):
        if len(self.args) < 2:
            return None
        try:
            return list(struct.unpack(self.args[0], self.args[1]))
        except:
            return None


class ReCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        if isinstance(self.args[0], re.Match):
            try:
                return getattr(self.args[0], self.name[4:])(*tuple(self.args[1:]))
            except:
                return None
        if not isinstance(self.args[0], str) or not self.args[0]:
            return None

        if self.args[0] == "/":
            index = self.args[0].rindex("/")
            pattern, flags = self.args[1: index], re.DOTALL
            for fc in self.args[index + 1:]:
                flags |= re.RegexFlag.__members__.get(fc.upper(), 0)
        else:
            pattern, flags = self.args[0], re.DOTALL
        r = re.compile(pattern, flags)
        try:
            return getattr(r, self.name[4:])(self.args[1], *tuple(self.args[2:]))
        except:
            return None
