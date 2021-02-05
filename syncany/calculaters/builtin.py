# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import math
import hashlib
import datetime
import pytz
import json
from tzlocal import get_localzone
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
            return "ObjectId"

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

        result = self.args[0]
        for i in range(1, len(self.args)):
            result += self.args[i]

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

        result = self.args[0]
        for i in range(1, len(self.args)):
            result -= self.args[i]

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

        result = self.args[0]
        for i in range(1, len(self.args)):
            result *= self.args[i]

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

        result = self.args[0]
        for i in range(1, len(self.args)):
            result /= self.args[i]

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

        result = self.args[0]
        for i in range(1, len(self.args)):
            result = result % self.args[i]

        return result

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
            if self.args[1] == ">>":
                return self.args[0] >> self.args[2]
            if self.args[1] == "<<":
                return self.args[0] << self.args[2]
            if self.args[1] == "&":
                return self.args[0] & self.args[2]
            if self.args[1] == "|":
                return self.args[0] | self.args[2]
            if self.args[1] == "^":
                return self.args[0] ^ self.args[2]

        if len(self.args) == 2:
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
    def split(self, datas):
        result = []
        for data in datas:
            if isinstance(data, str):
                result.extend(data.split(self.args[0]))
            elif isinstance(data, bytes):
                result.extend(data.decode("utf-8").split(self.args[0]))
            elif isinstance(data, list):
                for cdata in data:
                    result.extend(self.split(cdata))
            elif isinstance(data, dict):
                for key, value in data.items():
                    result.extend(self.split(key))
                    result.extend(self.split(value))
            else:
                result.extend(str(data).split(self.args[0]))

        return result

    def calculate(self):
        if not self.args:
            return []

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str) and isinstance(self.args[2], str):
            result = []
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[2] in data \
                        and isinstance(data[self.args[2]], str):
                    result.extend(data[self.args[2]].split(self.args[2]))
            return result

        return self.split(self.args[1:])

class JoinCalculater(Calculater):
    def join(self, datas, join_key, dict_join_key):
        result = []
        for data in datas:
            if isinstance(data, str):
                result.append(data)
            elif isinstance(data, bytes):
                result.append(data.decode("utf-8"))
            elif isinstance(data, list):
                result.append(self.join(data, join_key, dict_join_key))
            elif isinstance(data, dict):
                for key, value in data.items():
                    result.append((dict_join_key or join_key).join([self.join(key, join_key, dict_join_key), self.join(value, join_key, dict_join_key)]))
            else:
                result.append(str(data))

        return join_key.join(result)

    def calculate(self):
        if not self.args:
            return []

        if len(self.args) == 3 and isinstance(self.args[0], list) \
                and isinstance(self.args[1], str) and isinstance(self.args[2], str):
            result = []
            for data in self.args[0]:
                if isinstance(data, dict) and self.args[2] in data \
                        and isinstance(data[self.args[2]], str):
                    result.append(data[self.args[2]])
            return self.args[1].join(result)

        join_key, dict_join_key = "", None
        if isinstance(self.args[0], list):
            join_key = str(self.args[0][0])
            if len(self.args[0]) >= 2:
                dict_join_key = str(self.args[0][1])
        else:
            join_key = str(self.args[0])
        return self.join(self.args[1:], join_key, dict_join_key)


class NowCalculater(Calculater):
    TIMEDELTAS = {"Y": 365 * 24 * 60 * 60, "m": 30 * 24 * 60 * 60, "d": 24 * 60 * 60, "H": 60 * 60, "M": 60, "S": 1}

    def calculate(self):
        if not self.args:
            return datetime.datetime.now(tz=get_localzone())

        if self.args[0] and self.args[0][0] in ("-", "+") and self.args[0][-1] in ("Y", "m", "d", "H", "M", "S"):
            try:
                seconds = int(self.args[0][1:-1]) * self.TIMEDELTAS[self.args[0][-1]]
            except:
                return datetime.datetime.now(tz=get_localzone())

            if len(self.args) >= 2:
                now = datetime.datetime.now(tz=pytz.timezone(self.args[1]))
            else:
                now = datetime.datetime.now(tz=get_localzone())

            if self.args[0][0] == "-":
                return now - datetime.timedelta(seconds=seconds)
            return now + datetime.timedelta(seconds=seconds)

        return datetime.datetime.now(tz=pytz.timezone(self.args[0]))

class GtCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        result = self.args[0]
        for i in range(1, len(self.args)):
            if result <= self.args[i]:
                return False

        return True

class GteCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        result = self.args[0]
        for i in range(1, len(self.args)):
            if result < self.args[i]:
                return False

        return True

class LtCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        result = self.args[0]
        for i in range(1, len(self.args)):
            if result >= self.args[i]:
                return False

        return True

class LteCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        result = self.args[0]
        for i in range(1, len(self.args)):
            if result > self.args[i]:
                return False

        return True

class EqCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        result = self.args[0]
        for i in range(1, len(self.args)):
            if result != self.args[i]:
                return False

        return True

class NeqCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        result = self.args[0]
        for i in range(1, len(self.args)):
            if result == self.args[i]:
                return False

        return True

class AndCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        result = self.args[0]
        for i in range(1, len(self.args)):
            result = result and self.args[i]
        return result

class OrCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        result = self.args[0]
        for i in range(1, len(self.args)):
            result = result or self.args[i]
        return result

class InCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        result = self.args[0]
        for i in range(1, len(self.args)):
            if result not in self.args[i]:
                return False

        return True

class MaxCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        if not isinstance(self.args[0], list):
            if len(self.args) == 2:
                return self.args[1]
            if len(self.args) > 2:
                return max(*tuple(self.args))
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
            return max(*tuple([max_key_value] + list(self.args[1:])))
        return max_key_value

class MinCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        if not isinstance(self.args[0], list):
            if len(self.args) == 2:
                return self.args[1]
            if len(self.args) >= 2:
                return min(*tuple(self.args))
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
            return min(*tuple([min_key_value] + list(self.args[1:])))
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

        if isinstance(self.args[1], list):
            for data in self.args[1]:
                if len(self.args) >= 3:
                    if self.args[2] not in data:
                        continue
                    if data[self.args[2]] == self.args[0]:
                        return data

                if data == self.args[0]:
                    return data

            if isinstance(self.args[0], (int, float)):
                return self.args[2][int(self.args[0])] if len(self.args[2]) > self.args[0] else None

        elif isinstance(self.args[1], dict):
            if len(self.args) >= 3:
                if self.args[2] not in self.args[1]:
                    return None

                if self.args[1][self.args[2]] == self.args[0]:
                    return self.args[1][self.args[2]]

            if self.args[0] in self.args[1]:
                return self.args[1][self.args[0]]

        return None

class FilterCalculater(Calculater):
    def calculate(self):
        if not self.args or len(self.args) < 2:
            return []

        result = []
        if isinstance(self.args[0], list):
            for data in self.args[0]:
                if isinstance(data, dict) and len(self.args) >= 3:
                    if self.args[2] not in data:
                        continue
                    if data[self.args[2]] == self.args[1]:
                        result.append(data)
                elif data == self.args[1]:
                    return data

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

        keys = str(self.args[2]).split(".") if len(self.args) >= 3 else []
        def sort_key(x):
            for k in keys:
                if isinstance(x, dict) and k in x:
                    x = x[k]

            if isinstance(x, (list, dict)):
                return id(x)
            return x
        return sorted(self.args[0], key=sort_key,
                      reverse=True if len(self.args) >= 2 and self.args[1] else False)



class StringCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return ''

        func_name = self.name[8:]
        if isinstance(self.args[0], str) and hasattr(self.args[0], func_name):
            try:
                return getattr(self.args[0], func_name)(*tuple(self.args[1:]))
            except:
                return ''
        return ''

class ArrayCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        func_name = self.name[7:]
        if isinstance(self.args[0], list):
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
            if isinstance(arg, bytes):
                b += str(arg).encode("utf-8")
            else:
                b += arg

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