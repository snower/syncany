# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .calculater import Calculater

class AddCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        result = self.args[0]
        for i in range(1, len(self.args)):
            result += self.args[i]

        return result

class SubCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

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
