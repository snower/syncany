# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import datetime
from ..filters import DateTimeFilter, DateFilter, TimeFilter


class Calculater(object):
    def __init__(self, name, *args):
        self.type_cls = None
        self.name = name
        self.args = args

    def format_type(self, value):
        if value is None:
            if self.type_cls:
                if issubclass(self.type_cls, str):
                    return ""
                if issubclass(self.type_cls, (int, float)):
                    return 0
            return value
        if self.type_cls is None:
            self.type_cls = type(value)
            return value
        if isinstance(value, self.type_cls):
            return value
        if issubclass(self.type_cls, datetime.date):
            if issubclass(self.type_cls, datetime.datetime):
                return DateTimeFilter().filter(value)
            return DateFilter().filter(value)
        if issubclass(self.type_cls, datetime.time):
            return TimeFilter().filter(value)
        return self.type_cls(value)

    def calculate(self):
        if not self.args:
            return None

        if len(self.args) == 1:
            return self.args[0]

        return self.args

    def get_key_value(self, key, data):
        keys = key.split(".")
        for key in keys:
            if isinstance(data, list):
                if key[0] != ":":
                    return data

                slices = key.split(":")
                try:
                    start = int(slices[1])
                except:
                    start = 0

                if len(slices) <= 2:
                    data = data[start]
                    continue

                try:
                    end = int(slices[1])
                except:
                    end = len(data)

                try:
                    step = int(slices[1])
                except:
                    step = 1

                data = data[start: end: step]

            elif isinstance(data, dict):
                if key not in data:
                    return data
                data = data[key]
        return data


class TypingCalculater(Calculater):
    def get_datas(self):
        return self.args[0] if len(self.args) >= 1 else None

    def calculate(self):
        datas = self.get_datas()
        if datas is None:
            return None
        if not isinstance(datas, list):
            return self.typing_calculate(self.format_type(datas))

        result_datas = []
        for value in datas:
            result_datas.append(self.typing_calculate(self.format_type(value)))
        return result_datas

    def typing_calculate(self, value):
        return value


class MathematicalCalculater(Calculater):
    def get_left_datas(self):
        return self.args[0] if len(self.args) >= 1 else None

    def get_right_datas(self):
        return self.args[1] if len(self.args) >= 2 else None

    def get_args_datas(self):
        return self.args[2:]

    def get_default_value(self):
        return None

    def get_mathematical_value(self, data, i):
        if not isinstance(data, (list, tuple, set)):
            return self.format_type(data)
        if len(data) <= i:
            return None
        if data[i] is None:
            return None
        return self.format_type(data[i])

    def calculate(self):
        left_datas, right_datas = self.get_left_datas(), self.get_right_datas()
        if left_datas is None and right_datas is None:
            return self.get_default_value()
        is_left_list, is_right_list = isinstance(left_datas, (list, tuple, set)), isinstance(right_datas, (list, tuple, set))
        if not is_left_list and not is_right_list:
            result = self.mathematical_calculate(self.format_type(left_datas) if left_datas is not None else None,
                                                 self.format_type(right_datas) if right_datas is not None else None)
            args_datas = self.get_args_datas()
            if not args_datas:
                return result
            for args_data in args_datas:
                result = self.mathematical_calculate(result, self.format_type(args_data))
            return result

        max_size, datas = max(len(left_datas) if is_left_list else 1, len(right_datas) if is_right_list else 1), []
        for i in range(max_size):
            datas.append(self.mathematical_calculate(self.get_mathematical_value(left_datas, i),
                                                     self.get_mathematical_value(right_datas, i)))
        return datas

    def mathematical_calculate(self, left_value, right_value):
        return left_value
