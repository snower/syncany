# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import datetime
from ..utils import NumberTypes, SequenceTypes
from ..filters import DateTimeFilter, DateFilter, TimeFilter


class Calculater(object):
    _instances = {}

    @classmethod
    def instance(cls, name):
        instance_id = (cls.__name__, id(cls), name)
        if instance_id not in cls._instances:
            cls._instances[instance_id] = cls(name)
        return cls._instances[instance_id]

    def __init__(self, name):
        self.name = name

    def calculate(self, *args):
        if not args:
            return None
        if len(args) == 1:
            return args[0]
        return args

    def get_final_filter(self):
        return None

    def is_realtime_calculater(self):
        return False


class TypeFormatCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(TypeFormatCalculater, self).__init__(*args, **kwargs)

        self.type_cls = None

    def format_type(self, value):
        if value is None:
            if self.type_cls:
                if issubclass(self.type_cls, str):
                    return ""
                if issubclass(self.type_cls, NumberTypes):
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

    def calculate(self, *args):
        self.type_cls = None


class TypingCalculater(TypeFormatCalculater):
    def get_datas(self, args):
        return args[0] if len(args) >= 1 else None

    def calculate(self, *args):
        self.type_cls = None

        datas = self.get_datas(args)
        if datas is None:
            return None
        if not isinstance(datas, list):
            return self.typing_calculate(self.format_type(datas), args)

        result_datas = []
        for value in datas:
            result_datas.append(self.typing_calculate(self.format_type(value), args))
        return result_datas

    def typing_calculate(self, value, args):
        return value


class MathematicalCalculater(TypeFormatCalculater):
    def get_left_datas(self, args):
        return args[0] if len(args) >= 1 else None

    def get_right_datas(self, args):
        return args[1] if len(args) >= 2 else None

    def get_args_datas(self, args):
        return args[2:]

    def get_default_value(self):
        return None

    def get_mathematical_value(self, data, i):
        if not isinstance(data, SequenceTypes):
            return self.format_type(data)
        if len(data) <= i:
            return None
        if data[i] is None:
            return None
        return self.format_type(data[i])

    def calculate(self, *args):
        self.type_cls = None

        left_datas, right_datas = self.get_left_datas(args), self.get_right_datas(args)
        if left_datas is None and right_datas is None:
            return self.get_default_value()
        is_left_list, is_right_list = isinstance(left_datas, SequenceTypes), isinstance(right_datas, SequenceTypes)
        if not is_left_list and not is_right_list:
            result = self.mathematical_calculate(self.format_type(left_datas) if left_datas is not None else None,
                                                 self.format_type(right_datas) if right_datas is not None else None,
                                                 args)
            args_datas = self.get_args_datas(args)
            if not args_datas:
                return result
            for args_data in args_datas:
                result = self.mathematical_calculate(result, self.format_type(args_data), args)
            return result

        max_size, datas = max(len(left_datas) if is_left_list else 1, len(right_datas) if is_right_list else 1), []
        for i in range(max_size):
            datas.append(self.mathematical_calculate(self.get_mathematical_value(left_datas, i),
                                                     self.get_mathematical_value(right_datas, i), args))
        return datas

    def mathematical_calculate(self, left_value, right_value, args):
        return left_value
