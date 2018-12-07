# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import datetime
import pytz
from tzlocal import get_localzone
from .calculater import Calculater

class AddCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

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

        result = self.args[0]
        for i in range(1, len(self.args)):
            result *= self.args[i]

        return result

class DivCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        result = self.args[0]
        for i in range(1, len(self.args)):
            result /= self.args[i]

        return result

class ConcatCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return ""

        result = []
        for arg in self.args:
            if isinstance(arg, str):
                result.append(arg)
            elif isinstance(arg, bytes):
                result.append(arg.decode("utf-8"))
            else:
                result.append(str(arg))

        return "".join(result)

class SubstringCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return ""

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
            elif isinstance(data, (list, tuple, set)):
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

        return self.split(self.args[1:])

class JoinCalculater(Calculater):
    def join(self, datas, join_key, dict_join_key):
        result = []
        for data in datas:
            if isinstance(data, str):
                result.append(data)
            elif isinstance(data, bytes):
                result.append(data.decode("utf-8"))
            elif isinstance(data, (list, tuple, set)):
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

        join_key, dict_join_key = "", None
        if isinstance(self.args[0], list):
            join_key = str(self.args[0][0])
            if len(self.args[0]) >= 2:
                dict_join_key = str(self.args[0][1])
        else:
            join_key = str(self.args[0])
        return self.join(self.args[1:], join_key, dict_join_key)


class NowCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return datetime.datetime.now(tz=get_localzone())

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

        if not isinstance(self.args[0], (list, tuple, set)):
            if len(self.args) == 2:
                return self.args[1]
            if len(self.args) > 2:
                return max(*tuple(self.args))
            return self.args[0]

        max_key_value = max(*tuple(self.args[0]))

        if len(self.args) >= 2:
            if isinstance(self.args[1], (list, tuple, set)):
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

        if not isinstance(self.args[0], (list, tuple, set)):
            if len(self.args) == 2:
                return self.args[1]
            if len(self.args) >= 2:
                return min(*tuple(self.args))
            return self.args[0]

        min_key_value = min(*tuple(self.args[0]))

        if len(self.args) >= 2:
            if isinstance(self.args[1], (list, tuple, set)):
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

        if isinstance(self.args[1], (list, tuple, set)):
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
                    return self.args[1]

            if self.args[1] == self.args[0]:
                return self.args[1]

        return None